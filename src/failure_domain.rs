use crate::stringify_debug;
use crate::util::Uppercase;
use std::collections::HashMap;

////////////////////////////////////////////////////////////////////////////////
/// Failure domains of a given instance.
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

    /// Empty `FailureDomain` doesn't intersect with any other `FailureDomain`
    /// even with another empty one.
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
