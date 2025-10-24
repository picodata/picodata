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
/// `Vclock` is a mapping ([`HashMap`][std::collections::HashMap]) of
/// replica id (`usize`) to its LSN (`u64`).
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
/// Since vclocks do not form a total order some vclock instances might
/// be incomparible, leading to both `>=` and `<=` returning `false`.
/// Such situations can be detected by directly calling `partial_cmp`
/// and checking if it returns `None`.
///
/// ```no_run
/// use tarantool::vclock::Vclock;
/// use std::cmp::PartialOrd;
///
/// let vc1 = Vclock::from([0, 100]);
/// let vc2 = Vclock::from([100, 0]);
/// assert_eq!(vc1 <= vc2, false);
/// assert_eq!(vc1 >= vc2, false);
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
    #[inline(always)]
    pub fn current() -> Self {
        lua_state()
            .eval("return box.info.vclock")
            .expect("this should be called after box.cfg")
    }

    /// Obtains current vclock from Tarantool `box.info.vclock` API.
    ///
    /// Returns an error if `box.cfg{ .. }` was not called yet.
    ///
    #[inline(always)]
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
    #[inline(always)]
    pub fn ignore_zero(mut self) -> Self {
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
    #[inline(always)]
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
    #[inline(always)]
    pub fn get(&self, index: usize) -> Lsn {
        self.0.get(&index).copied().unwrap_or(0)
    }

    /// Does a component-wise comparison of `self` against `other`.
    ///
    /// If `ignore_zero` is `true`, the component at index `0` is ignored. This
    /// is useful for comparing vclocks from different replicas, because the
    /// zeroth component only tracks local updates.
    #[inline]
    pub fn cmp(&self, other: &Self, ignore_zero: bool) -> Option<Ordering> {
        let mut le = true;
        let mut ge = true;

        for i in self.0.keys().chain(other.0.keys()) {
            if ignore_zero && *i == 0 {
                continue;
            }
            let a: Lsn = self.0.get(i).copied().unwrap_or(0);
            let b: Lsn = other.0.get(i).copied().unwrap_or(0);
            le = le && a <= b;
            ge = ge && a >= b;
            if !ge && !le {
                return None;
            }
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

    /// Does a component-wise comparison of `self` against `other`.
    ///
    /// Ignores the components at index `0`.
    ///
    /// See also [`Self::cmp`].
    #[inline(always)]
    pub fn cmp_ignore_zero(&self, other: &Self) -> Option<Ordering> {
        self.cmp(other, true)
    }
}

impl<const N: usize> From<[Lsn; N]> for Vclock {
    /// Converts an array `[Lsn; N]` into a `Vclock`, skipping
    /// components with LSN equal to `0`.
    ///
    /// Primarily used for testing. It has no meaningful application in
    /// the real world.
    #[inline]
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
    /// Does a component-wise comparison of `self` against `other`.
    ///
    /// Includes the components at index `0` in the comparison, so it's probably
    /// not suitable for comparing vclocks from different replicas.
    ///
    /// See also [`Self::cmp_ignore_zero`].
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cmp(other, false)
    }
}

impl<L> LuaRead<L> for Vclock
where
    L: AsLua,
{
    #[inline(always)]
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

    #[inline(always)]
    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        HashMap::push_to_lua(&self.0, lua).map_err(|_| unreachable!())
    }
}

impl<L: AsLua> PushInto<L> for Vclock {
    type Err = Void;

    #[inline(always)]
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
        // Vclock from hash map.
        assert_eq!(
            Vclock::from([0, 0, 12, 0]).into_inner(),
            HashMap::from([(2, 12)])
        );

        assert_eq!(
            Vclock::from([99, 101]).ignore_zero().into_inner(),
            HashMap::from([(1, 101)])
        );

        let vc_011 = Vclock::from([0, 1, 1]);
        let vc_012 = Vclock::from([0, 1, 2]);
        let vc_021 = Vclock::from([0, 2, 1]);
        let vc_211 = Vclock::from([2, 1, 1]);
        let vc_112 = Vclock::from([1, 1, 2]);
        let vc_121 = Vclock::from([1, 2, 1]);

        //
        // Compare including zeroth component.
        //

        assert_eq!(vc_011, vc_011);

        assert_ne!(vc_011, vc_012);
        assert_ne!(vc_012, vc_021);
        assert_ne!(vc_021, vc_011);

        assert!(vc_021 > vc_011);
        assert!(vc_012 > vc_011);
        assert_eq!(vc_012.partial_cmp(&vc_021), None);

        assert!(vc_011 < vc_211);
        assert!(vc_012 < vc_112);
        assert!(vc_021 < vc_121);

        //
        // Compare ignoring zeroth component.
        //

        assert_eq!(vc_211.cmp(&vc_112, false), None);
        assert_eq!(vc_112.cmp(&vc_121, false), None);
        assert_eq!(vc_121.cmp(&vc_211, false), None);

        assert_eq!(vc_211.cmp_ignore_zero(&vc_211), Some(Ordering::Equal));

        assert_eq!(vc_211.cmp_ignore_zero(&vc_112), Some(Ordering::Less));
        assert_eq!(vc_112.cmp_ignore_zero(&vc_121), None);
        assert_eq!(vc_121.cmp_ignore_zero(&vc_211), Some(Ordering::Greater));

        // Vclock from array.
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
        let vc: Vclock = rmp_serde::from_slice(&mp).unwrap();
        assert_eq!(vc, Vclock::from([0, 0, 0, 30]));

        // Serialize
        assert_eq!(rmp_serde::to_vec(&vc).unwrap(), mp);

        let invalid_mp = b"\x81\x00\xa0"; // {[0] = ""}
        let err: Result<Vclock, _> = rmp_serde::from_slice(invalid_mp);
        assert_eq!(
            err.unwrap_err().to_string(),
            "invalid type: string \"\", expected u64"
        )
    }
}
