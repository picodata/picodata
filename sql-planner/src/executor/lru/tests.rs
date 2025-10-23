use super::{Cache, LRUCache};
use crate::errors::SbroadError;
use crate::ir::Plan;
use pretty_assertions::assert_eq;
use smol_str::{format_smolstr, ToSmolStr};

#[test]
fn lru1() {
    let mut cache: LRUCache<usize, Plan> = LRUCache::new(2, None).unwrap();
    cache.put(1, Plan::default()).unwrap();
    cache.put(2, Plan::default()).unwrap();
    assert_eq!(cache.get(&1).unwrap().is_some(), true);
    cache.put(3, Plan::default()).unwrap();
    assert_eq!(cache.get(&2).unwrap().is_none(), true);
    assert_eq!(cache.get(&3).unwrap().is_some(), true);
    assert_eq!(cache.get(&1).unwrap().is_some(), true);
}

#[test]
fn lru2() {
    let cache_err: Result<LRUCache<usize, Plan>, SbroadError> = LRUCache::new(0, None);
    assert_eq!(cache_err.is_err(), true);
}

#[test]
fn lru3() {
    let evict_fn = Box::new(|_: &usize, value: &mut String| {
        let value_old = value.clone();
        value.push_str("_old");
        Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
            "changed {value_old} to {value} during cache eviction"
        )))
    });
    let mut cache: LRUCache<usize, String> = LRUCache::new(1, Some(evict_fn)).unwrap();
    cache.put(1, "one".to_string()).unwrap();
    assert_eq!(
        SbroadError::UnexpectedNumberOfValues(
            "changed one to one_old during cache eviction".to_smolstr()
        ),
        cache.put(2, "two".to_string()).unwrap_err()
    );
}

#[test]
fn lru4() {
    let mut cache: LRUCache<usize, String> = LRUCache::new(2, None).unwrap();
    cache.put(1, "one".to_string()).unwrap();
    cache.put(1, "two".to_string()).unwrap();
    assert_eq!(cache.get(&1).unwrap(), Some(&"two".to_string()));
}
