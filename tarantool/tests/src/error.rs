use tarantool::error::TarantoolError;
use tarantool::space::Space;

use crate::common::S1Record;

pub fn error_last() {
    let space = Space::find("test_s1").unwrap();
    let input = S1Record {
        id: 0,
        text: "".to_string(),
    };
    space.truncate().unwrap();
    space.insert(&input).unwrap();

    let result = space.insert(&input);
    assert!(result.is_err());
    assert!(TarantoolError::maybe_last().is_err());
}
