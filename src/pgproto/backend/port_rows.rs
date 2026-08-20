//! Lazy access to the rows an executed statement has produced.
//!
//! Instead of decoding the whole result set into a `Vec<Vec<PgValue>>` upfront,
//! the port the statement was executed into is kept around, and every row is
//! decoded right before it goes onto the wire, see
//! [`crate::pgproto::backend::result::Rows`].

use crate::pgproto::error::PgResult;
use crate::sql::port::PicoPortOwned;
use std::rc::Rc;
use tarantool::ffi::sql::{PortC, PortCCursor};

/// A port pinned at a stable address for as long as its rows are needed.
///
/// `PortC` is address-sensitive: once the first entry is added, the port's
/// `first` and `last` pointers point into the `first_entry` field embedded in
/// the port itself, so moving a non-empty port leaves them dangling.
///
/// [`PinnedPort::fill`] is the only constructor, which makes that impossible to
/// get wrong: the port is moved into the `Rc` allocation while it's still empty
/// and is filled in place afterwards. `Rc` never moves its contents, and we
/// never take the port back out of it.
#[derive(Clone)]
pub struct PinnedPort(Rc<PicoPortOwned>);

impl PinnedPort {
    /// Create an empty port and execute something into it.
    pub fn fill(fill: impl FnOnce(&mut PicoPortOwned) -> PgResult<()>) -> PgResult<Self> {
        let mut port = Rc::new(PicoPortOwned::new());

        // The port is still empty, hence moving it into the `Rc` above was fine.
        // This is the last time it's borrowed mutably: from now on it's shared,
        // so it can neither be moved nor mutated.
        let port_mut = Rc::get_mut(&mut port).expect("the port is not shared yet");
        fill(port_mut)?;

        Ok(Self(port))
    }

    #[inline(always)]
    pub fn port_c(&self) -> &PortC {
        self.0.port_c()
    }

    /// The number of msgpacks stored in the port.
    #[inline(always)]
    fn size(&self) -> usize {
        self.port_c().size() as usize
    }
}

/// An iterator over the rows stored in a port.
///
/// The rows are not decoded here: the iterator yields raw msgpack borrowed from
/// the port, which stays valid for as long as this handle is alive.
pub struct PortRows {
    port: PinnedPort,
    cursor: PortCCursor,
    /// The number of rows left. The port knows how many msgpacks it holds, so
    /// nothing has to be decoded to count the rows.
    remaining: usize,
}

impl PortRows {
    /// Rows of a DQL result. The first msgpack of such a port is the metadata,
    /// the remaining ones are the tuples, so the first one is skipped.
    fn new_dql(port: PinnedPort) -> Self {
        let mut cursor = port.port_c().cursor();

        // SAFETY: the cursor was created from this very port, which can't be
        // mutated anymore, see `PinnedPort`. The same holds for `next_row`.
        let metadata = unsafe { cursor.next(port.port_c()) };
        debug_assert!(metadata.is_some(), "a DQL port must contain metadata");

        let remaining = port.size().saturating_sub(1);
        Self {
            port,
            cursor,
            remaining,
        }
    }

    /// The msgpack of the next row.
    fn next_row(&mut self) -> Option<&[u8]> {
        // Destructured to borrow the port and the cursor independently.
        let Self {
            port,
            cursor,
            remaining,
        } = self;

        if *remaining == 0 {
            return None;
        }
        *remaining -= 1;

        // SAFETY: see `new_dql`.
        unsafe { cursor.next(port.port_c()) }
    }

    /// Take the first `count` rows out, leaving the rest in `self`.
    ///
    /// Nothing is decoded: the returned iterator only remembers where its part
    /// of the port begins and how many rows it covers.
    fn split_off_front(&mut self, count: usize) -> Self {
        let count = count.min(self.remaining);
        let head = Self {
            port: self.port.clone(),
            cursor: self.cursor,
            remaining: count,
        };

        // Skipping over the entries is just a walk over a linked list.
        for _ in 0..count {
            self.next_row();
        }

        head
    }
}

/// A row which hasn't been decoded yet.
pub enum Row<'a> {
    /// A msgpack array with a value per column, borrowed from the port.
    Tuple(&'a [u8]),
    /// A line of an EXPLAIN, handed over by value: explain output is tiny, so
    /// there's nothing to gain from keeping the line alive to borrow it from.
    ExplainLine(String),
}

/// The rows a portal is going to send to the client.
pub enum RowSource {
    /// DQL rows, stored in a port as msgpack and decoded on demand.
    Port(PortRows),
    /// The output of an EXPLAIN. A single port entry holds a number of explain
    /// lines, so there's no one-to-one mapping between entries and rows here.
    /// Explains are tiny, so their lines are materialized once and streamed
    /// from memory.
    ///
    /// Note: the lines are stored back to front, so that yielding one is a pop.
    Explain(Vec<String>),
}

impl RowSource {
    /// Rows of a DQL result, decoded from the port they were executed into.
    pub fn dql(port: PinnedPort) -> Self {
        Self::Port(PortRows::new_dql(port))
    }

    pub fn explain(lines: impl IntoIterator<Item = String>) -> Self {
        let mut lines: Vec<_> = lines.into_iter().collect();
        lines.reverse();
        Self::Explain(lines)
    }

    /// The number of rows left.
    pub fn len(&self) -> usize {
        match self {
            Self::Port(rows) => rows.remaining,
            Self::Explain(lines) => lines.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The next row, still encoded.
    pub fn next_row(&mut self) -> Option<Row<'_>> {
        match self {
            Self::Port(rows) => rows.next_row().map(Row::Tuple),
            Self::Explain(lines) => lines.pop().map(Row::ExplainLine),
        }
    }

    /// Take the first `count` rows out, leaving the rest in `self`.
    pub fn split_off_front(&mut self, count: usize) -> Self {
        match self {
            Self::Port(rows) => Self::Port(rows.split_off_front(count)),
            Self::Explain(lines) => {
                let count = count.min(lines.len());
                // The lines are stored back to front, so the first `count` of
                // them are the last `count` elements, in the very same order.
                Self::Explain(lines.split_off(lines.len() - count))
            }
        }
    }
}

mod tests {
    use super::*;
    use crate::pgproto::value::PgValue;
    use postgres_types::Type;
    use sql::executor::Port as _;
    use std::borrow::Cow;
    use std::io::Cursor;

    /// A port shaped like the one a DQL leaves behind: the metadata comes first,
    /// followed by a msgpack per row.
    fn dql_port(rows: &[Vec<u8>]) -> PinnedPort {
        PinnedPort::fill(|port| {
            // The content of the metadata doesn't matter here, it's skipped.
            port.add_mp(b"\x90");
            for row in rows {
                port.add_mp(row);
            }
            Ok(())
        })
        .unwrap()
    }

    /// A single column row: `[n]`.
    fn row(n: i64) -> Vec<u8> {
        let mut mp = Vec::new();
        rmp::encode::write_array_len(&mut mp, 1).unwrap();
        rmp::encode::write_sint(&mut mp, n).unwrap();
        mp
    }

    /// A row's text is decoded where it lies in the port, which is what makes
    /// it cheap to decode a row right before it's sent.
    #[::tarantool::test]
    fn a_row_string_is_borrowed_from_the_port() {
        let mut mp = Vec::new();
        rmp::encode::write_array_len(&mut mp, 1).unwrap();
        rmp::encode::write_str(&mut mp, "hello").unwrap();

        // Note: the port copies what it's given, so `mp` and the row the port
        // hands back are two distinct buffers.
        let mut source = RowSource::dql(dql_port(std::slice::from_ref(&mp)));
        let Some(Row::Tuple(msgpack)) = source.next_row() else {
            panic!("expected a row");
        };

        let mut cursor = Cursor::new(msgpack);
        rmp::decode::read_array_len(&mut cursor).unwrap();
        let value = PgValue::decode_mp(&mut cursor, &Type::TEXT).unwrap();
        let PgValue::Text(Cow::Borrowed(text)) = value else {
            panic!("expected a borrowed text, got {value:?}");
        };

        assert_eq!(text, "hello");
        assert!(msgpack.as_ptr_range().contains(&text.as_ptr()));
        assert!(!mp.as_ptr_range().contains(&text.as_ptr()));
    }

    #[::tarantool::test]
    fn cursor_yields_the_same_msgpacks_as_iter() {
        let rows: Vec<_> = (0..5).map(row).collect();
        let port = dql_port(&rows);

        let mut cursor = port.port_c().cursor();
        let mut iter = port.port_c().iter();
        loop {
            // SAFETY: the cursor was created from this very port, which is
            // never mutated once it's pinned.
            let from_cursor = unsafe { cursor.next(port.port_c()) };
            assert_eq!(from_cursor, iter.next());
            if from_cursor.is_none() {
                break;
            }
        }
    }

    #[::tarantool::test]
    fn split_off_front_shares_the_port() {
        let rows: Vec<_> = (0..5).map(row).collect();
        let mut tail = PortRows::new_dql(dql_port(&rows));

        let mut head = tail.split_off_front(2);
        assert_eq!(head.remaining, 2);
        assert_eq!(tail.remaining, 3);

        // The tail continues right where the head ends.
        assert_eq!(tail.next_row(), Some(rows[2].as_slice()));

        // The head is still readable after the last other handle is gone: the
        // rows it yields belong to the port, which they both keep alive.
        drop(tail);
        assert_eq!(head.next_row(), Some(rows[0].as_slice()));
        assert_eq!(head.next_row(), Some(rows[1].as_slice()));
        assert_eq!(head.next_row(), None);
    }
}
