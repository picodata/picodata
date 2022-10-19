#![cfg(feature = "picodata")]

use tarantool::tuple::Tuple;

pub fn tuple_format_get_names() {
    let space = tarantool::space::Space::find("test_s2").unwrap();
    let idx_1 = space.index("idx_1").unwrap();
    let tuple = idx_1.get(&("key_16",)).unwrap().unwrap();
    let format = tuple.format();

    let names = format.names().collect::<Vec<_>>();
    assert_eq!(vec!["id", "key", "value", "a", "b"], names);
}

pub fn tuple_as_named_buffer() {
    let space = tarantool::space::Space::find("test_s2").unwrap();
    let idx_1 = space.index("idx_1").unwrap();
    let tuple = idx_1.get(&("key_16",)).unwrap().unwrap();

    let mp_map = tuple.as_named_buffer().unwrap();
    let map: rmpv::Value = rmp_serde::from_slice(&mp_map).unwrap();
    let map = map.as_map().unwrap();

    assert_eq!(5, map.len());
    for (k, v) in map {
        match k.as_str().unwrap() {
            "id" => assert_eq!(16, v.as_u64().unwrap()),
            "key" => assert_eq!("key_16", v.as_str().unwrap()),
            "value" => assert_eq!("value_16", v.as_str().unwrap()),
            "a" => assert_eq!(1, v.as_u64().unwrap()),
            "b" => assert_eq!(3, v.as_u64().unwrap()),
            _ => {unreachable!()}
        }
    }

    let tuple = Tuple::new(&(1, "foo")).unwrap();
    let mp_map = tuple.as_named_buffer().unwrap();
    let map: rmpv::Value = rmp_serde::from_slice(&mp_map).unwrap();
    let map = map.as_map().unwrap();

    assert_eq!(2, map.len());
    for (k, v) in map {
        match k.as_u64().unwrap() {
            0 => assert_eq!(1, v.as_u64().unwrap()),
            1 => assert_eq!("foo", v.as_str().unwrap()),
            _ => {unreachable!()}
        }
    }
}

