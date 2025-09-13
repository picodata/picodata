use comfy_table::{Cell, ContentArrangement, Row, Table};
use rmp::decode::{read_array_len, read_int, read_str_len};
use std::io::{Cursor, Error as IoError, Result as IoResult};
use std::str::from_utf8_unchecked;

struct PlanLine<'mp> {
    select_id: i64,
    order: i64,
    from: i64,
    detail: &'mp [u8],
}

impl PlanLine<'_> {
    fn is_location(&self) -> bool {
        self.select_id == -1 && self.order == -1 && self.from == -1
    }

    fn is_sql(&self) -> bool {
        self.select_id == -2 && self.order == -2 && self.from == -2
    }

    fn as_row(&self) -> Row {
        let cells = [
            Cell::new(self.select_id),
            Cell::new(self.order),
            Cell::new(self.from),
            Cell::new(String::from_utf8_lossy(self.detail)),
        ];
        Row::from(cells)
    }
}

fn try_parse_plan_line(mp: &[u8]) -> IoResult<PlanLine<'_>> {
    let (select_id, order, from, start, len) = {
        let mut cur = Cursor::new(mp);
        let len = read_array_len(&mut cur).map_err(IoError::other)?;
        if len != 4 {
            return Err(IoError::other(format!(
                "Expected query plan msgpack to be an array of length 4, but got {len}",
            )));
        }
        let select_id: i64 = read_int(&mut cur).map_err(IoError::other)?;
        let order: i64 = read_int(&mut cur).map_err(IoError::other)?;
        let from: i64 = read_int(&mut cur).map_err(IoError::other)?;
        let detail_len = read_str_len(&mut cur).map_err(IoError::other)?;
        let detail_start = cur.position() as usize;
        (select_id, order, from, detail_start, detail_len as usize)
    };
    Ok(PlanLine {
        select_id,
        order,
        from,
        detail: &mp[start..start + len],
    })
}

#[derive(Default)]
pub struct PlanBlock {
    location: String,
    sql: String,
    table: Table,
}

impl std::fmt::Display for PlanBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}\n{}\n{}\n", self.location, self.sql, self.table)
    }
}

pub struct PlanBlockIter<'bytes, I>
where
    I: Iterator<Item = &'bytes [u8]>,
{
    port: I,
    state: Option<PlanBlock>,
    idx: u64,
}

impl<'bytes, I> PlanBlockIter<'bytes, I>
where
    I: Iterator<Item = &'bytes [u8]>,
{
    pub fn new(port: I) -> Self {
        PlanBlockIter {
            port,
            state: None,
            idx: 0,
        }
    }
}

impl<'bytes, I> Iterator for PlanBlockIter<'bytes, I>
where
    I: Iterator<Item = &'bytes [u8]>,
{
    type Item = PlanBlock;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(mp) = self.port.next() else {
                match self.state.as_mut() {
                    // Empty port, empty state - exit.
                    None => return None,
                    Some(state) => {
                        // Port is over, return the state.
                        let plan = std::mem::take(state);
                        self.state = None;
                        return Some(plan);
                    }
                }
            };
            let Ok(line) = try_parse_plan_line(mp) else {
                // Something wrong with our msgpack, let's stop
                // iterations instead of panic.
                return None;
            };
            // Handle a new plan block.
            if line.is_location() {
                let mut plan = PlanBlock::default();
                self.idx += 1;
                plan.location = format!("{}. Query ({}):", self.idx, unsafe {
                    from_utf8_unchecked(line.detail)
                });
                plan.table
                    .set_content_arrangement(ContentArrangement::Dynamic);
                plan.table
                    .set_header(["selectid", "order", "from", "detail"]);
                match self.state.as_mut() {
                    None => self.state = Some(plan),
                    Some(state) => {
                        std::mem::swap(state, &mut plan);
                        return Some(plan);
                    }
                }
            } else if line.is_sql() {
                match self.state.as_mut() {
                    // Hmmm, the port iterator lost the beginning of the plan block.
                    // Skip it and wait for the beginning of a new one.
                    None => continue,
                    Some(state) => {
                        state.sql = unsafe { String::from_utf8_unchecked(line.detail.to_vec()) }
                    }
                }
            } else {
                match self.state.as_mut() {
                    // Hmmm, the port iterator lost the beginning of the plan block.
                    // Skip it and wait for the beginning of a new one.
                    None => continue,
                    Some(state) => {
                        let _ = state.table.add_row(line.as_row());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_port() -> Vec<Vec<u8>> {
        let mut port = Vec::new();

        port.push(b"\x94\xff\xff\xff\xa8location".to_vec());
        port.push(b"\x94\xfe\xfe\xfe\xa3sql".to_vec());
        port.push(b"\x94\x00\x00\x00\xa5line0".to_vec());
        port.push(b"\x94\x01\x01\x01\xa5line1".to_vec());

        port.push(b"\x94\xff\xff\xff\xa8location".to_vec());
        port.push(b"\x94\xfe\xfe\xfe\xa3sql".to_vec());
        port.push(b"\x94\x00\x00\x00\xa5line0".to_vec());
        port.push(b"\x94\x01\x01\x01\xa5line1".to_vec());

        port
    }

    fn mock_table() -> Table {
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(["selectid", "order", "from", "detail"]);
        table
            .add_row([Cell::new(0), Cell::new(0), Cell::new(0), Cell::new("line0")])
            .add_row([Cell::new(1), Cell::new(1), Cell::new(1), Cell::new("line1")]);

        table
    }

    #[test]
    fn test_plan_block_iter() {
        let port = mock_port();
        let mut iter = PlanBlockIter::new(port.iter().map(|v| v.as_slice()));

        let block = iter.next().unwrap();
        assert_eq!(block.location, "1. Query (location):");
        assert_eq!(block.sql, "sql");
        assert_eq!(block.table.to_string(), mock_table().to_string());

        let block = iter.next().unwrap();
        assert_eq!(block.location, "2. Query (location):");
        assert_eq!(block.sql, "sql");
        assert_eq!(block.table.to_string(), mock_table().to_string());

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_plan_block_to_string() {
        let block = PlanBlock {
            location: "location".to_string(),
            sql: "sql".to_string(),
            table: mock_table(),
        };
        let expected = "\
            location\n\
            sql\n\
            +----------+-------+------+--------+\n\
            | selectid | order | from | detail |\n\
            +==================================+\n\
            | 0        | 0     | 0    | line0  |\n\
            |----------+-------+------+--------|\n\
            | 1        | 1     | 1    | line1  |\n\
            +----------+-------+------+--------+\n\
        ";
        assert_eq!(block.to_string(), expected);
    }
}
