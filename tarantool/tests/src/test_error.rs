use tarantool_module::{Error, Space};
use crate::common::S1Record;

pub fn test_error_last() {
    let mut space = Space::find_by_name("test_s1").unwrap().unwrap();
    let input = S1Record{ id: 0, text: "".to_string() };
    space.truncate().unwrap();
    space.insert(&input, false).unwrap();

    let result = space.insert(&input, false);
    assert!(result.is_err());
    assert!(Error::last().is_err());
}
