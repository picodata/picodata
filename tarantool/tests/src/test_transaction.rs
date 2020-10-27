use std::io;

use tarantool_module::error::Error;
use tarantool_module::space::Space;
use tarantool_module::transaction::start_transaction;

use crate::common::S1Record;

pub fn test_transaction_commit() {
    let mut space = Space::find_by_name("test_s1").unwrap().unwrap();
    space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "test".to_string(),
    };

    let result = start_transaction(|| -> Result<(), Error> {
        space.insert(&input, false)?;
        Ok(())
    });
    assert!(result.is_ok());

    let output = space.primary_key().get(&(1,)).unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().into_struct::<S1Record>().unwrap(), input);
}

pub fn test_transaction_rollback() {
    let mut space = Space::find_by_name("test_s1").unwrap().unwrap();
    space.truncate().unwrap();

    let result = start_transaction(|| -> Result<(), Error> {
        space.insert(
            &S1Record {
                id: 1,
                text: "test".to_string(),
            },
            false,
        )?;
        Err(Error::IO(io::ErrorKind::Interrupted.into()))
    });
    assert!(result.is_err());

    let output = space.primary_key().get(&(1,)).unwrap();
    assert!(output.is_none());
}
