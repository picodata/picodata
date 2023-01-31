use crate::stringify_debug;
use crate::traft::Distance;
use crate::util::Uppercase;
use std::collections::{HashMap, HashSet};

/// Failure domain of a given instance.
///
/// A failure domain is a set of `KEY=VALUE` pairs that we call
/// components. Specifying failure domains provides a user a way to mark
/// two instances as being a single point of failure. Both key and value
/// can be anything meaningful to a user, like `"region=eu"`, or
/// `"dc=msk"`.
///
/// If two different failure domains have at least one common component
/// (both key and value), it is assumed they both may encounter outage
/// at once.
///
/// There might be more than one component in a failure domain, e.g.
/// `"dc=msk,srv=msk-1"`. Yet Picodata doesn't make any assumptions
/// about their meaning or hierarchy, it only matches components.
///
/// Picodata relies on failure domains to enhance fault tolerance of a
/// cluster:
///
/// - Picodata will avoid forming a replicaset of instances with at
///   least one common failure domain component.
///
/// - Picodata tends to keep raft voters as distant from each other as
///   possible.
///
/// Failure domains are case-insensitive. Components are converted to
/// upprcase implicitly.
///
#[derive(Default, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct FailureDomain {
    #[serde(flatten)]
    data: HashMap<Uppercase, Uppercase>,
}

impl FailureDomain {
    pub fn contains_name(&self, name: &Uppercase) -> bool {
        self.data.contains_key(name)
    }

    pub fn names(&self) -> std::collections::hash_map::Keys<Uppercase, Uppercase> {
        self.data.keys()
    }

    /// Checks whether `self` and `other` failure domains have at least
    /// one common component.
    ///
    /// Empty failure domain doesn't intersect with any other even with
    /// another empty one.
    ///
    /// # Example
    ///
    /// ```
    /// use picodata::failure_domain::FailureDomain;
    ///
    /// let msk_1 = FailureDomain::from([("dc", "msk"), ("srv", "msk-1")]);
    /// let msk_2 = FailureDomain::from([("dc", "msk"), ("srv", "msk-2")]);
    /// let spb = FailureDomain::from([("dc", "spb")]);
    ///
    /// assert_eq!(msk_1.intersects(&msk_2), true);
    /// assert_eq!(msk_1.intersects(&spb), false);
    /// ```
    ///
    pub fn intersects(&self, other: &Self) -> bool {
        for (name, value) in &self.data {
            match other.data.get(name) {
                Some(other_value) if value == other_value => {
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    /// Calculates a distance between `self` and `other` failure domains.
    ///
    /// The distance is a number of components differ.
    ///
    /// # Example
    ///
    /// ```
    /// use picodata::failure_domain::FailureDomain;
    ///
    /// let msk_1 = FailureDomain::from([("dc", "msk"), ("srv", "msk-1")]);
    /// let msk_2 = FailureDomain::from([("dc", "msk"), ("srv", "msk-2")]);
    /// let spb = FailureDomain::from([("dc", "spb")]);
    ///
    /// assert_eq!(msk_1.distance(&msk_1), 0);
    /// assert_eq!(msk_1.distance(&msk_2), 1);
    /// assert_eq!(msk_1.distance(&spb), 2);
    /// ```
    pub fn distance(&self, other: &Self) -> Distance {
        let mut keys: HashSet<&Uppercase> = HashSet::new();
        keys.extend(self.names());
        keys.extend(other.names());
        keys.iter()
            .filter(|&&key| self.data.get(key) != other.data.get(key))
            .count() as u64
    }
}

impl std::fmt::Display for FailureDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("{")?;
        let mut iter = self.data.iter();
        if let Some((k, v)) = iter.next() {
            write!(f, "{k}: {v}")?;
            for (k, v) in iter {
                write!(f, ", {k}: {v}")?;
            }
        }
        f.write_str("}")?;
        Ok(())
    }
}

impl std::fmt::Debug for FailureDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut ds = f.debug_struct(stringify_debug!(FailureDomain));
        for (name, value) in &self.data {
            ds.field(name, &**value);
        }
        ds.finish()
    }
}

impl<I, K, V> From<I> for FailureDomain
where
    I: IntoIterator<Item = (K, V)>,
    Uppercase: From<K>,
    Uppercase: From<V>,
{
    fn from(data: I) -> Self {
        Self {
            data: data
                .into_iter()
                .map(|(k, v)| (Uppercase::from(k), Uppercase::from(v)))
                .collect(),
        }
    }
}

impl<'a> IntoIterator for &'a FailureDomain {
    type IntoIter = <&'a HashMap<Uppercase, Uppercase> as IntoIterator>::IntoIter;
    type Item = <&'a HashMap<Uppercase, Uppercase> as IntoIterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::FailureDomain;

    #[test]
    fn test_intersection() {
        let empty = FailureDomain::default();
        let dc_msk = FailureDomain::from([("dc", "msk")]);
        let dc_spb = FailureDomain::from([("dc", "spb")]);
        let srv_s1 = FailureDomain::from([("srv", "s1")]);

        assert!(!empty.intersects(&empty));
        assert!(!empty.intersects(&dc_msk));
        assert!(!dc_msk.intersects(&dc_spb));
        assert!(!dc_msk.intersects(&srv_s1));
        assert!(dc_msk.intersects(&dc_msk));

        // check case sensivity
        #[allow(non_snake_case)]
        let dc_SPB = FailureDomain::from([("dc", "SPB")]);
        assert!(dc_spb.intersects(&dc_SPB));

        let dcsrv = FailureDomain::from([("dc", "msk"), ("srv", "s1")]);
        assert!(dcsrv.intersects(&dc_msk));
        assert!(dcsrv.intersects(&srv_s1));
    }

    #[test]
    fn test_distance() {
        let empty = FailureDomain::default();
        let dc_msk = FailureDomain::from([("dc", "msk")]);
        let dc_spb = FailureDomain::from([("dc", "spb")]);
        let srv_s1 = FailureDomain::from([("srv", "s1")]);

        assert_eq!(empty.distance(&empty), 0);
        assert_eq!(empty.distance(&dc_msk), 1);
        assert_eq!(dc_msk.distance(&dc_spb), 1);
        assert_eq!(dc_msk.distance(&srv_s1), 2);

        let dcsrv = FailureDomain::from([("dc", "msk"), ("srv", "s1")]);
        assert_eq!(dcsrv.distance(&dc_msk), 1);
        assert_eq!(dcsrv.distance(&srv_s1), 1);
        assert_eq!(dcsrv.distance(&dc_spb), 2);
        assert_eq!(dcsrv.distance(&empty), 2);
    }

    #[test]
    fn doctest_intersects() {
        let msk_1 = FailureDomain::from([("dc", "msk"), ("srv", "msk-1")]);
        let msk_2 = FailureDomain::from([("dc", "msk"), ("srv", "msk-2")]);
        let spb = FailureDomain::from([("dc", "spb")]);

        assert_eq!(msk_1.intersects(&msk_2), true);
        assert_eq!(msk_1.intersects(&spb), false);
    }

    #[test]
    fn doctest_distance() {
        let msk_1 = FailureDomain::from([("dc", "msk"), ("srv", "msk-1")]);
        let msk_2 = FailureDomain::from([("dc", "msk"), ("srv", "msk-2")]);
        let spb = FailureDomain::from([("dc", "spb")]);

        assert_eq!(msk_1.distance(&msk_1), 0);
        assert_eq!(msk_1.distance(&msk_2), 1);
        assert_eq!(msk_1.distance(&spb), 2);
    }
}
