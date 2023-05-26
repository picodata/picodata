//! Vector clock.
//!
//! This module provides two concepts:
//!
//! - type alias [`Lsn`] = `u64`,
//! - and struct [`Vclock`].
//!
//! Their meaning is explained below.
//!
//! To ensure data persistence, Tarantool records updates to the
//! database in the so-called write-ahead log (WAL) files. Each record
//! in the WAL represents a single Tarantool data-change request such as
//! `INSERT`, `UPDATE`, or `DELETE`, and is assigned a monotonically
//! growing log sequence number (LSN).
//!
//! Enabling replication makes all replicas in a replica set to exchange
//! their records, each with it's own LSN. Together, LSNs from different
//! replicas form a vector clock (vclock). Vclock defines the database
//! state of an instance.
//!
//! The zero vclock component is special, it's used for tracking local
/// changes that aren't replicated.
///
use std::cmp::Ordering;
use std::collections::HashMap;
use std::num::NonZeroI32;

use serde::{Deserialize, Serialize};
use tlua::{Push, PushInto, PushOne, PushOneInto, Void};

use crate::lua_state;
use crate::tlua::{AsLua, LuaRead, ReadResult};

/// Tarantool log sequence number.
pub type Lsn = u64;

/// Tarantool vector clock.
///
/// Find the explanation of the concept in the [module
/// documentation][self].
///
/// `Vclock` is a mapping of replica id (`usize`) to its LSN (`u64`).
/// Components with LSN equal to `0` are skipped during
/// (de)serialization.
///
/// Unlike in Tarantool, `Vclock` doesn't impose any restrictions on the
/// replica ids (in Tarantool its valid range is `0..32`).
///
/// `Vclock` supports equality comparison ([`Eq`][std::cmp::Eq]) and
/// partial ordering ([`PartialOrd`][std::cmp::PartialOrd]). Two vclocks
/// are said to be `a => b` if and only if for every component `i` it's
/// true that `a[i] => b[i]`. Missing components are treated as `0`.
///
/// ```no_run
/// use tarantool::vclock::Vclock;
///
/// let vc1 = Vclock::from([1, 9, 88]);
/// let vc2 = Vclock::from([1, 10, 100]);
/// assert!(vc1 < vc2);
/// ```
///
/// Since vclocks doesn't form a total order, the use of operators `>`,
/// `<`, `>=`, `<=` is discouraged as they panic on incomparable values.
///
/// ```no_run
/// use tarantool::vclock::Vclock;
/// use std::cmp::PartialOrd;
///
/// let vc1 = Vclock::from([0, 100]);
/// let vc2 = Vclock::from([100, 0]);
/// assert!(vc1.partial_cmp(&vc2).is_none());
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Vclock(HashMap<usize, Lsn>);

impl Vclock {
    /// Obtains current vclock from Tarantool `box.info.vclock` API.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tarantool::vclock::Vclock;
    /// dbg!(Vclock::current());
    /// ```
    ///
    /// Example output:
    ///
    /// ```text
    /// Vclock({0: 2, 1: 101})
    /// ```
    ///
    /// # Panics
    ///
    /// If `box.cfg{ .. }` was not called yet.
    ///
    pub fn current() -> Self {
        lua_state()
            .eval("return box.info.vclock")
            .expect("this should be called after box.cfg")
    }

    /// Obtains current vclock from Tarantool `box.info.vclock` API.
    ///
    /// Returns an error if `box.cfg{ .. }` was not called yet.
    ///
    pub fn try_current() -> Result<Self, tlua::LuaError> {
        lua_state().eval("return box.info.vclock")
    }

    /// Sets zero component to 0. It's used for tracking local updates
    /// that aren't replicated so it should be excluded from comparison
    /// of vclocks of different replicas.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tarantool::vclock::Vclock;
    /// let vc = Vclock::from([100, 1]);
    /// assert_eq!(vc.ignore_zero(), Vclock::from([0, 1]));
    /// ```
    ///
    pub fn ignore_zero(mut self) -> Self {
        println!("{self:?}");
        self.0.remove(&0);
        self
    }

    /// Consumes the `Vclock`, returning underlying `HashMap`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tarantool::vclock::Vclock;
    /// # use std::collections::HashMap;
    /// let vc = Vclock::from([0, 0, 200]);
    /// assert_eq!(vc.into_inner(), HashMap::from([(2, 200)]))
    /// ```
    pub fn into_inner(self) -> HashMap<usize, Lsn> {
        self.0
    }

    /// Returns an [`Lsn`] at `index` or zero if it is not present.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tarantool::vclock::Vclock;
    /// # use std::collections::HashMap;
    /// let vc = Vclock::from([0, 10]);
    /// assert_eq!(vc.get(0), 0);
    /// assert_eq!(vc.get(1), 10);
    /// assert_eq!(vc.get(2), 0);
    /// ```
    pub fn get(&self, index: usize) -> Lsn {
        self.0.get(&index).copied().unwrap_or(0)
    }
}

impl<const N: usize> From<[Lsn; N]> for Vclock {
    /// Primarily used for testing. It has no meaningful application in
    /// the real world.
    fn from(from: [Lsn; N]) -> Self {
        Self(
            from.iter()
                .copied()
                .enumerate()
                .filter(|(_, lsn)| *lsn != 0)
                .collect(),
        )
    }
}

impl PartialOrd for Vclock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut le = true;
        let mut ge = true;

        for i in self.0.keys().chain(other.0.keys()) {
            let a: Lsn = self.0.get(i).copied().unwrap_or(0);
            let b: Lsn = other.0.get(i).copied().unwrap_or(0);
            le = le && a <= b;
            ge = ge && a >= b;
        }

        if le && ge {
            Some(Ordering::Equal)
        } else if le && !ge {
            Some(Ordering::Less)
        } else if !le && ge {
            Some(Ordering::Greater)
        } else {
            None
        }
    }
}

impl<L> LuaRead<L> for Vclock
where
    L: AsLua,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> ReadResult<Self, L> {
        match HashMap::lua_read_at_position(lua, index) {
            Ok(v) => Ok(Self(v)),
            Err((l, err)) => {
                let err = err
                    .when("converting Lua table to Vclock")
                    .expected("{[i] = lsn}");
                Err((l, err))
            }
        }
    }
}

impl<L: AsLua> Push<L> for Vclock {
    type Err = Void;

    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        HashMap::push_to_lua(&self.0, lua).map_err(|_| unreachable!())
    }
}

impl<L: AsLua> PushInto<L> for Vclock {
    type Err = Void;

    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        self.push_to_lua(lua)
    }
}

impl<L: AsLua> PushOne<L> for Vclock {}
impl<L: AsLua> PushOneInto<L> for Vclock {}

#[cfg(feature = "internal_test")]
mod tests {
    use std::collections::HashMap;

    use crate::lua_state;

    use super::*;

    #[crate::test(tarantool = "crate")]
    fn test_vclock_current() {
        let space_name = crate::temp_space_name!();
        let space = crate::space::Space::builder(&space_name).create().unwrap();
        space.index_builder("pk").create().unwrap();

        let mut vc = Vclock::current();

        space.insert(&(1,)).unwrap();

        vc.0.entry(1).and_modify(|v| *v += 1);
        assert_eq!(Vclock::current(), vc);
    }

    #[crate::test(tarantool = "crate")]
    #[allow(clippy::eq_op)]
    fn test_vclock_cmp() {
        assert_eq!(
            Vclock::from([0, 0, 12, 0]).into_inner(),
            HashMap::from([(2, 12)])
        );

        assert_eq!(
            Vclock::from([99, 101]).ignore_zero().into_inner(),
            HashMap::from([(1, 101)])
        );

        let vc_11 = Vclock::from([0, 1, 1]);
        let vc_12 = Vclock::from([0, 1, 2]);
        let vc_21 = Vclock::from([0, 2, 1]);

        assert_eq!(vc_11, vc_11);

        assert_ne!(vc_11, vc_12);
        assert_ne!(vc_12, vc_21);
        assert_ne!(vc_21, vc_11);

        assert!(vc_21 > vc_11);
        assert!(vc_12 > vc_11);
        assert_eq!(vc_12.partial_cmp(&vc_21), None);

        assert!(Vclock::from([100, 200]) > Vclock::from([100]));
        assert!(Vclock::from([1, 10, 100]) > Vclock::from([1, 9, 88]));
    }

    #[crate::test(tarantool = "crate")]
    fn test_vclock_luaread() {
        let l = lua_state();
        let luaread = |s| l.eval::<Vclock>(s);

        assert_eq!(luaread("return {}").unwrap(), Vclock::from([]));

        assert_eq!(luaread("return {[0] = 100}").unwrap(), Vclock::from([100]));

        assert_eq!(
            luaread("return {101, 102}").unwrap(),
            Vclock::from([0, 101, 102])
        );

        assert_eq!(
            luaread("return {[33] = 103}").unwrap(),
            Vclock(HashMap::from([(33, 103)]))
        );

        assert_eq!(
            luaread("return {[1] = 'help'}").unwrap_err().to_string(),
            "failed reading Lua value: u64 expected, got string
    while converting Lua table to Vclock: {[i] = lsn} expected, got table value of wrong type
    while reading value(s) returned by Lua: tarantool::vclock::Vclock expected, got table"
        );

        assert_eq!(
            luaread("return {foo = 16}").unwrap_err().to_string(),
            "failed reading Lua value: usize expected, got string
    while converting Lua table to Vclock: {[i] = lsn} expected, got table key of wrong type
    while reading value(s) returned by Lua: tarantool::vclock::Vclock expected, got table"
        );

        assert_eq!(
            luaread("return 'not-a-vclock'").unwrap_err().to_string(),
            "failed converting Lua table to Vclock: {[i] = lsn} expected, got string
    while reading value(s) returned by Lua: tarantool::vclock::Vclock expected, got string"
        );
    }

    #[crate::test(tarantool = "crate")]
    fn test_vclock_luapush() {
        let l = lua_state();

        let lsns: HashMap<usize, Lsn> = l
            .eval_with("return ...", Vclock::from([100, 0, 102]))
            .unwrap();
        assert_eq!(lsns, HashMap::from([(0, 100), (2, 102)]));
    }

    #[crate::test(tarantool = "crate")]
    fn test_vclock_serde() {
        let mp = rmp_serde::to_vec(&HashMap::from([(3, 30)])).unwrap();
        assert_eq!(mp, b"\x81\x03\x1e"); // {[3] = 30}

        // Deserialize
        let vc: Vclock = rmp_serde::from_read_ref(&mp).unwrap();
        assert_eq!(vc, Vclock::from([0, 0, 0, 30]));

        // Serialize
        assert_eq!(rmp_serde::to_vec(&vc).unwrap(), mp);

        let invalid_mp = b"\x81\x00\xa0"; // {[0] = ""}
        let err: Result<Vclock, _> = rmp_serde::from_read_ref(invalid_mp);
        assert_eq!(
            err.unwrap_err().to_string(),
            "invalid type: string \"\", expected u64"
        )
    }
}
