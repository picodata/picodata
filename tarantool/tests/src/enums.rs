use tarantool::{
    space::{SpaceEngineType, SpaceFieldType},
    index::{IndexType, IndexFieldType, RtreeIndexDistanceType}
};

use serde_plain::{to_string, from_str};

pub fn space_engine_type() {
    type T = SpaceEngineType;
    assert_eq!(to_string(&T::Vinyl).unwrap(), "vinyl");
    assert_eq!(from_str::<T>("memtx").unwrap(), T::Memtx);
    assert_eq!(from_str::<T>("Memtx ").unwrap(), T::Memtx);
    assert_eq!(from_str::<T>(" MEMTX ").unwrap(), T::Memtx);
    assert_eq!(from_str::<T>("mmtx").ok(), None);

    assert_eq!(to_string(&T::Memtx).unwrap(), "memtx");
    assert_eq!(from_str::<T>("vinyl").unwrap(), T::Vinyl);
    assert_eq!(from_str::<T>("Vinyl ").unwrap(), T::Vinyl);
    assert_eq!(from_str::<T>(" VINYL ").unwrap(), T::Vinyl);
    assert_eq!(from_str::<T>("wekneel").ok(), None);
}

pub fn space_field_type() {
    type T = SpaceFieldType;
    assert_eq!(to_string(&T::Any).unwrap(), "any");
    assert_eq!(from_str::<T>("any").unwrap(), T::Any);
    assert_eq!(from_str::<T>("Any  ").unwrap(), T::Any);
    assert_eq!(from_str::<T>(" ANY ").unwrap(), T::Any);
    assert_eq!(from_str::<T>("ny").ok(), None);

    assert_eq!(to_string(&T::Unsigned).unwrap(), "unsigned");
    assert_eq!(from_str::<T>("unsigned").unwrap(), T::Unsigned);
    assert_eq!(from_str::<T>(" Unsigned  ").unwrap(), T::Unsigned);
    assert_eq!(from_str::<T>(" UnsigneD ").unwrap(), T::Unsigned);
    assert_eq!(from_str::<T>("nsigned").ok(), None);

    assert_eq!(to_string(&T::String).unwrap(), "string");
    assert_eq!(from_str::<T>("string").unwrap(), T::String);
    assert_eq!(from_str::<T>(" String  ").unwrap(), T::String);
    assert_eq!(from_str::<T>(" STRING ").unwrap(), T::String);
    assert_eq!(from_str::<T>("str").ok(), None);

    assert_eq!(to_string(&T::Number).unwrap(), "number");
    assert_eq!(from_str::<T>("number").unwrap(), T::Number);
    assert_eq!(from_str::<T>(" Number  ").unwrap(), T::Number);
    assert_eq!(from_str::<T>(" NUMBER ").unwrap(), T::Number);
    assert_eq!(from_str::<T>("num").ok(), None);

    assert_eq!(to_string(&T::Double).unwrap(), "double");
    assert_eq!(from_str::<T>("double").unwrap(), T::Double);
    assert_eq!(from_str::<T>("Double  ").unwrap(), T::Double);
    assert_eq!(from_str::<T>(" DOUBLE ").unwrap(), T::Double);
    assert_eq!(from_str::<T>("doubl").ok(), None);

    assert_eq!(to_string(&T::Integer).unwrap(), "integer");
    assert_eq!(from_str::<T>("integer").unwrap(), T::Integer);
    assert_eq!(from_str::<T>("Integer  ").unwrap(), T::Integer);
    assert_eq!(from_str::<T>(" INTEGER ").unwrap(), T::Integer);
    assert_eq!(from_str::<T>("int").ok(), None);

    assert_eq!(to_string(&T::Boolean).unwrap(), "boolean");
    assert_eq!(from_str::<T>("boolean").unwrap(), T::Boolean);
    assert_eq!(from_str::<T>("Boolean  ").unwrap(), T::Boolean);
    assert_eq!(from_str::<T>(" BOOLEAN ").unwrap(), T::Boolean);
    assert_eq!(from_str::<T>("bool").ok(), None);

    assert_eq!(to_string(&T::Varbinary).unwrap(), "varbinary");
    assert_eq!(from_str::<T>("varbinary").unwrap(), T::Varbinary);
    assert_eq!(from_str::<T>("binary").ok(), None);

    assert_eq!(to_string(&T::Decimal).unwrap(), "decimal");
    assert_eq!(from_str::<T>("decimal").unwrap(), T::Decimal);
    assert_eq!(from_str::<T>("Decimal  ").unwrap(), T::Decimal);
    assert_eq!(from_str::<T>(" DECIMAL ").unwrap(), T::Decimal);
    assert_eq!(from_str::<T>("dec").ok(), None);

    assert_eq!(to_string(&T::Uuid).unwrap(), "uuid");
    assert_eq!(from_str::<T>("uuid").unwrap(), T::Uuid);
    assert_eq!(from_str::<T>("Uuid  ").unwrap(), T::Uuid);
    assert_eq!(from_str::<T>(" UUID ").unwrap(), T::Uuid);
    assert_eq!(from_str::<T>("uid").ok(), None);

    assert_eq!(to_string(&T::Datetime).unwrap(), "datetime");
    assert_eq!(from_str::<T>("datetime").unwrap(), T::Datetime);
    assert_eq!(from_str::<T>("time").ok(), None);

    assert_eq!(to_string(&T::Interval).unwrap(), "interval");
    assert_eq!(from_str::<T>("interval").unwrap(), T::Interval);
    assert_eq!(from_str::<T>("duration").ok(), None);

    assert_eq!(to_string(&T::Array).unwrap(), "array");
    assert_eq!(from_str::<T>("array").unwrap(), T::Array);
    assert_eq!(from_str::<T>("Array  ").unwrap(), T::Array);
    assert_eq!(from_str::<T>(" ARRAY ").unwrap(), T::Array);
    assert_eq!(from_str::<T>("arr").ok(), None);

    assert_eq!(to_string(&T::Scalar).unwrap(), "scalar");
    assert_eq!(from_str::<T>("scalar").unwrap(), T::Scalar);
    assert_eq!(from_str::<T>("Scalar  ").unwrap(), T::Scalar);
    assert_eq!(from_str::<T>(" SCALAR ").unwrap(), T::Scalar);
    assert_eq!(from_str::<T>("scal").ok(), None);

    assert_eq!(to_string(&T::Map).unwrap(), "map");
    assert_eq!(from_str::<T>("map").unwrap(), T::Map);
    assert_eq!(from_str::<T>("dict").ok(), None);
}

pub fn index_type() {
    type T = IndexType;
    assert_eq!(to_string(&T::Hash).unwrap(), "hash");
    assert_eq!(from_str::<T>("hash").unwrap(), T::Hash);
    assert_eq!(from_str::<T>("Hash  ").unwrap(), T::Hash);
    assert_eq!(from_str::<T>(" HASH ").unwrap(), T::Hash);
    assert_eq!(from_str::<T>("digest").ok(), None);

    assert_eq!(to_string(&T::Tree).unwrap(), "tree");
    assert_eq!(from_str::<T>("tree").unwrap(), T::Tree);
    assert_eq!(from_str::<T>("Tree  ").unwrap(), T::Tree);
    assert_eq!(from_str::<T>(" TREE ").unwrap(), T::Tree);
    assert_eq!(from_str::<T>("bush").ok(), None);

    assert_eq!(to_string(&T::Bitset).unwrap(), "bitset");
    assert_eq!(from_str::<T>("bitset").unwrap(), T::Bitset);
    assert_eq!(from_str::<T>("BitSet  ").unwrap(), T::Bitset);
    assert_eq!(from_str::<T>(" BITSET ").unwrap(), T::Bitset);
    assert_eq!(from_str::<T>("set").ok(), None);

    assert_eq!(to_string(&T::Rtree).unwrap(), "rtree");
    assert_eq!(from_str::<T>("rtree").unwrap(), T::Rtree);
    assert_eq!(from_str::<T>("RTree  ").unwrap(), T::Rtree);
    assert_eq!(from_str::<T>(" RTREE ").unwrap(), T::Rtree);
    assert_eq!(from_str::<T>("btree").ok(), None);
}

pub fn index_field_type() {
    type T = IndexFieldType;
    assert_eq!(to_string(&T::Unsigned).unwrap(), "unsigned");
    assert_eq!(from_str::<T>("unsigned").unwrap(), T::Unsigned);
    assert_eq!(from_str::<T>(" Unsigned  ").unwrap(), T::Unsigned);
    assert_eq!(from_str::<T>(" UnsigneD ").unwrap(), T::Unsigned);
    assert_eq!(from_str::<T>("nsigned").ok(), None);

    assert_eq!(to_string(&T::String).unwrap(), "string");
    assert_eq!(from_str::<T>("string").unwrap(), T::String);
    assert_eq!(from_str::<T>(" String  ").unwrap(), T::String);
    assert_eq!(from_str::<T>(" STRING ").unwrap(), T::String);
    assert_eq!(from_str::<T>("str").ok(), None);

    assert_eq!(to_string(&T::Integer).unwrap(), "integer");
    assert_eq!(from_str::<T>("integer").unwrap(), T::Integer);
    assert_eq!(from_str::<T>("Integer  ").unwrap(), T::Integer);
    assert_eq!(from_str::<T>(" INTEGER ").unwrap(), T::Integer);
    assert_eq!(from_str::<T>("int").ok(), None);

    assert_eq!(to_string(&T::Number).unwrap(), "number");
    assert_eq!(from_str::<T>("number").unwrap(), T::Number);
    assert_eq!(from_str::<T>(" Number  ").unwrap(), T::Number);
    assert_eq!(from_str::<T>(" NUMBER ").unwrap(), T::Number);
    assert_eq!(from_str::<T>("num").ok(), None);

    assert_eq!(to_string(&T::Double).unwrap(), "double");
    assert_eq!(from_str::<T>("double").unwrap(), T::Double);
    assert_eq!(from_str::<T>("Double  ").unwrap(), T::Double);
    assert_eq!(from_str::<T>(" DOUBLE ").unwrap(), T::Double);
    assert_eq!(from_str::<T>("doubl").ok(), None);

    assert_eq!(to_string(&T::Decimal).unwrap(), "decimal");
    assert_eq!(from_str::<T>("decimal").unwrap(), T::Decimal);
    assert_eq!(from_str::<T>("Decimal  ").unwrap(), T::Decimal);
    assert_eq!(from_str::<T>(" DECIMAL ").unwrap(), T::Decimal);
    assert_eq!(from_str::<T>("dec").ok(), None);

    assert_eq!(to_string(&T::Boolean).unwrap(), "boolean");
    assert_eq!(from_str::<T>("boolean").unwrap(), T::Boolean);
    assert_eq!(from_str::<T>("Boolean  ").unwrap(), T::Boolean);
    assert_eq!(from_str::<T>(" BOOLEAN ").unwrap(), T::Boolean);
    assert_eq!(from_str::<T>("bool").ok(), None);

    assert_eq!(to_string(&T::Varbinary).unwrap(), "varbinary");
    assert_eq!(from_str::<T>("varbinary").unwrap(), T::Varbinary);
    assert_eq!(from_str::<T>("Varbinary  ").unwrap(), T::Varbinary);
    assert_eq!(from_str::<T>(" VARBINARY ").unwrap(), T::Varbinary);
    assert_eq!(from_str::<T>("var").ok(), None);

    assert_eq!(to_string(&T::Uuid).unwrap(), "uuid");
    assert_eq!(from_str::<T>("uuid").unwrap(), T::Uuid);
    assert_eq!(from_str::<T>("Uuid  ").unwrap(), T::Uuid);
    assert_eq!(from_str::<T>(" UUID ").unwrap(), T::Uuid);
    assert_eq!(from_str::<T>("uid").ok(), None);

    assert_eq!(to_string(&T::Array).unwrap(), "array");
    assert_eq!(from_str::<T>("array").unwrap(), T::Array);
    assert_eq!(from_str::<T>("Array  ").unwrap(), T::Array);
    assert_eq!(from_str::<T>(" ARRAY ").unwrap(), T::Array);
    assert_eq!(from_str::<T>("arr").ok(), None);

    assert_eq!(to_string(&T::Scalar).unwrap(), "scalar");
    assert_eq!(from_str::<T>("scalar").unwrap(), T::Scalar);
    assert_eq!(from_str::<T>("Scalar  ").unwrap(), T::Scalar);
    assert_eq!(from_str::<T>(" SCALAR ").unwrap(), T::Scalar);
    assert_eq!(from_str::<T>("scal").ok(), None);
}

pub fn rtree_index_distance_type() {
    type T = RtreeIndexDistanceType;
    assert_eq!(to_string(&T::Euclid).unwrap(), "euclid");
    assert_eq!(from_str::<T>("euclid").unwrap(), T::Euclid);
    assert_eq!(from_str::<T>("Euclid  ").unwrap(), T::Euclid);
    assert_eq!(from_str::<T>(" EUCLID ").unwrap(), T::Euclid);
    assert_eq!(from_str::<T>("pythagoras").ok(), None);

    assert_eq!(to_string(&T::Manhattan).unwrap(), "manhattan");
    assert_eq!(from_str::<T>("manhattan").unwrap(), T::Manhattan);
    assert_eq!(from_str::<T>("Manhattan  ").unwrap(), T::Manhattan);
    assert_eq!(from_str::<T>(" MANHATTAN ").unwrap(), T::Manhattan);
    assert_eq!(from_str::<T>("queens").ok(), None);
}
