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

pub fn set_error() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn uordblks() -> i32 {
        unsafe { libc::mallinfo().uordblks }
    }

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    fn uordblks() -> i32 {
        0
    }

    // first call to box_set_error results in some memory allocation,
    // which we don't care about
    tarantool::set_error!(0, "oops");

    // first call to format with given format string results in some memory allocation,
    // which we don't care about
    format!("oops: {}", 0);

    // check that we don't leak any memory when calling set_error with format args
    let mem_before = uordblks();
    tarantool::set_error!(0, "oops: {}", 1);
    tarantool::set_error!(0, "oops: {}", 2);
    tarantool::set_error!(0, "oops: {}", 3);
    tarantool::set_error!(0, "oops: {}", 4);
    assert_eq!(mem_before, uordblks());
}
