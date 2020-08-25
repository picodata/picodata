use crate::common::{S2Record, S1Record};
use tarantool_module::Tuple;

pub fn test_tuple_new_from_struct() {
    let input = S1Record{
        id: 1,
        text: "text".to_string(),
    };
    assert!(Tuple::new_from_struct(&input).is_ok());
}

pub fn test_tuple_field_count() {
    // struct -> tuple
    let tuple = Tuple::new_from_struct(&S2Record{
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 2,
        b: 3
    }).unwrap();
    assert_eq!(tuple.field_count(), 5);

    // tuple w/ single field
    let tuple = Tuple::new_from_struct(&(0,)).unwrap();
    assert_eq!(tuple.field_count(), 1);

    // empty tuple
    let tuple = Tuple::new_from_struct::<Vec<()>>(&vec![]).unwrap();
    assert_eq!(tuple.field_count(), 0);
}

pub fn test_tuple_size() {
    let tuple = Tuple::new_from_struct(&S2Record{
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 2,
        b: 3
    }).unwrap();
    assert_eq!(tuple.size(), 14);
}

pub fn test_tuple_into_struct() {
    let input = S2Record{
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 2,
        b: 3
    };

    // 1:1 decode
    let tuple = Tuple::new_from_struct(&input).unwrap();
    let output: S2Record = tuple.into_struct().unwrap();
    assert_eq!(output, input);

    // partial decode (with trimming trailing fields)
    let tuple = Tuple::new_from_struct(&input).unwrap();
    let output: S1Record = tuple.into_struct().unwrap();
    assert_eq!(output, S1Record{ id: 1, text: "key".to_string() });
}

pub fn test_tuple_clone() {
    let tuple_1 = {
        let tuple_2 = Tuple::new_from_struct(&S1Record {
            id: 1,
            text: "text".to_string(),
        }).unwrap();
        tuple_2.clone()
    };
    assert!(tuple_1.into_struct::<S1Record>().is_ok());
}
