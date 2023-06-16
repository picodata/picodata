use std::io;

use tarantool::error::Error;
use tarantool::space::Space;
use tarantool::transaction::transaction;

use crate::common::S1Record;

pub fn transaction_commit() {
    let space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "test".to_string(),
    };

    let result = transaction(|| -> Result<(), Error> {
        space.insert(&input)?;
        Ok(())
    });
    assert!(result.is_ok());

    let output = space.get(&(1,)).unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap(), input);
}

pub fn transaction_rollback() {
    let space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let result = transaction(|| -> Result<(), Error> {
        space.insert(&S1Record {
            id: 1,
            text: "test".to_string(),
        })?;
        Err(Error::IO(io::ErrorKind::Interrupted.into()))
    });
    assert_eq!(
        result.unwrap_err().to_string(),
        "Transaction rolled-back: IO error: operation interrupted"
    );

    let output = space.get(&(1,)).unwrap();
    assert!(output.is_none());
}
