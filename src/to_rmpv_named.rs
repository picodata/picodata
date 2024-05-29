//! This module is copy pasted from rmpv::ext with the sole purpose to
//! implement serializing into [`rmpv::Value`] converting structs to maps
//! (instead of arrays, how [`rmpv::ext::to_value`] does it).

use rmpv::ext::Error;
use rmpv::Value;
use serde::ser;

struct RmpvNamedSerializer;

/// Convert a `T` into `rmpv::Value` which is an enum that can represent any valid MessagePack data.
///
/// Structs are converted to msgpack maps (not arrays).
///
/// This conversion can fail if `T`'s implementation of `Serialize` decides to fail.
///
/// ```rust
/// use picodata::to_rmpv_named::to_rmpv_named;
///
/// let val = to_rmpv_named("John Smith").unwrap();
///
/// assert_eq!(rmpv::Value::String("John Smith".into()), val);
/// ```
#[inline(always)]
pub fn to_rmpv_named<T: ser::Serialize>(value: T) -> Result<Value, Error> {
    value.serialize(RmpvNamedSerializer)
}

impl ser::Serializer for RmpvNamedSerializer {
    type Ok = Value;
    type Error = Error;

    type SerializeSeq = SerializeVec;
    type SerializeTuple = SerializeVec;
    type SerializeTupleStruct = SerializeVec;
    type SerializeTupleVariant = SerializeTupleVariant;
    type SerializeMap = DefaultSerializeMap;
    type SerializeStruct = DefaultSerializeMap;
    type SerializeStructVariant = SerializeStructVariant;

    #[inline]
    fn serialize_bool(self, val: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Boolean(val))
    }

    #[inline]
    fn serialize_i8(self, val: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(val as i64)
    }

    #[inline]
    fn serialize_i16(self, val: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(val as i64)
    }

    #[inline]
    fn serialize_i32(self, val: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(val as i64)
    }

    #[inline]
    fn serialize_i64(self, val: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::from(val))
    }

    #[inline]
    fn serialize_u8(self, val: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(val as u64)
    }

    #[inline]
    fn serialize_u16(self, val: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(val as u64)
    }

    #[inline]
    fn serialize_u32(self, val: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(val as u64)
    }

    #[inline]
    fn serialize_u64(self, val: u64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::from(val))
    }

    #[inline]
    fn serialize_f32(self, val: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::F32(val))
    }

    #[inline]
    fn serialize_f64(self, val: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::F64(val))
    }

    #[inline]
    fn serialize_char(self, val: char) -> Result<Self::Ok, Self::Error> {
        let mut buf = String::new();
        buf.push(val);
        self.serialize_str(&buf)
    }

    #[inline]
    fn serialize_str(self, val: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::String(val.into()))
    }

    #[inline]
    fn serialize_bytes(self, val: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Binary(val.into()))
    }

    #[inline(always)]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Nil)
    }

    #[inline(always)]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Nil)
    }

    #[inline(always)]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _idx: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Value::from(variant))
    }

    #[inline]
    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        if name == rmpv::MSGPACK_EXT_STRUCT_NAME {
            let mut ext_se = ExtSerializer::new();
            value.serialize(&mut ext_se)?;

            return ext_se.value();
        }

        to_rmpv_named(value)
    }

    #[inline]
    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _idx: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Ok(Value::Map(vec![(
            Value::from(variant),
            to_rmpv_named(value)?,
        )]))
    }

    #[inline(always)]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    #[inline(always)]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let se = SerializeVec {
            vec: Vec::with_capacity(len.unwrap_or(0)),
        };
        Ok(se)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        self.serialize_tuple(len)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _idx: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        let se = SerializeTupleVariant {
            variant,
            vec: Vec::with_capacity(len),
        };
        Ok(se)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        let se = DefaultSerializeMap {
            map: Vec::with_capacity(len.unwrap_or(0)),
            next_key: None,
        };
        Ok(se)
    }

    #[inline(always)]
    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        let se = DefaultSerializeMap {
            map: Vec::with_capacity(len),
            next_key: None,
        };
        Ok(se)
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _idx: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        let se = SerializeStructVariant {
            variant,
            map: Vec::with_capacity(len),
        };
        Ok(se)
    }
}

pub struct ExtSerializer {
    fields_se: Option<ExtFieldSerializer>,
}

impl ser::Serializer for &mut ExtSerializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = Self;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    #[cold]
    fn serialize_bytes(self, _val: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received bytes",
        ))
    }

    #[cold]
    fn serialize_bool(self, _val: bool) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received bool",
        ))
    }

    #[cold]
    fn serialize_i8(self, _value: i8) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom("expected tuple, received i8"))
    }

    #[cold]
    fn serialize_i16(self, _val: i16) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received i16",
        ))
    }

    #[cold]
    fn serialize_i32(self, _val: i32) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received i32",
        ))
    }

    #[cold]
    fn serialize_i64(self, _val: i64) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received i64",
        ))
    }

    #[cold]
    fn serialize_u8(self, _val: u8) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom("expected tuple, received u8"))
    }

    #[cold]
    fn serialize_u16(self, _val: u16) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received u16",
        ))
    }

    #[cold]
    fn serialize_u32(self, _val: u32) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received u32",
        ))
    }

    #[cold]
    fn serialize_u64(self, _val: u64) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received u64",
        ))
    }

    #[cold]
    fn serialize_f32(self, _val: f32) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received f32",
        ))
    }

    #[cold]
    fn serialize_f64(self, _val: f64) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received f64",
        ))
    }

    #[cold]
    fn serialize_char(self, _val: char) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received char",
        ))
    }

    #[cold]
    fn serialize_str(self, _val: &str) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received str",
        ))
    }

    #[cold]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received unit",
        ))
    }

    #[cold]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received unit_struct",
        ))
    }

    #[cold]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received unit_variant",
        ))
    }

    #[cold]
    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received newtype_struct",
        ))
    }

    #[cold]
    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received newtype_variant",
        ))
    }

    #[cold]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received none",
        ))
    }

    #[cold]
    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received some",
        ))
    }

    #[cold]
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received seq",
        ))
    }

    #[inline]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Error> {
        // FIXME check len
        self.fields_se = Some(ExtFieldSerializer::new());

        Ok(self)
    }

    #[cold]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received tuple_struct",
        ))
    }

    #[cold]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received tuple_variant",
        ))
    }

    #[cold]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received map",
        ))
    }

    #[cold]
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received struct",
        ))
    }

    #[cold]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(<Error as ser::Error>::custom(
            "expected tuple, received struct_variant",
        ))
    }
}

impl ser::SerializeTuple for &mut ExtSerializer {
    type Ok = ();
    type Error = Error;

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        match self.fields_se {
            Some(ref mut se) => value.serialize(&mut *se),
            None => unreachable!(),
        }
    }

    #[inline(always)]
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct ExtFieldSerializer {
    tag: Option<i8>,
    binary: Option<Vec<u8>>,
}

impl ser::Serializer for &mut ExtFieldSerializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    #[inline]
    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        if self.tag.is_none() {
            self.tag.replace(value);
            Ok(())
        } else {
            Err(<Error as ser::Error>::custom(
                "exptected i8 and bytes, received second i8",
            ))
        }
    }

    #[inline]
    fn serialize_bytes(self, val: &[u8]) -> Result<Self::Ok, Self::Error> {
        if self.binary.is_none() {
            self.binary.replace(val.to_vec());

            Ok(())
        } else {
            Err(<Error as ser::Error>::custom(
                "expected i8 and bytes, received second bytes",
            ))
        }
    }

    #[cold]
    fn serialize_bool(self, _val: bool) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received bool",
        ))
    }

    #[cold]
    fn serialize_i16(self, _val: i16) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received i16",
        ))
    }

    #[cold]
    fn serialize_i32(self, _val: i32) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received i32",
        ))
    }

    #[cold]
    fn serialize_i64(self, _val: i64) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received i64",
        ))
    }

    #[cold]
    fn serialize_u8(self, _val: u8) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received u8",
        ))
    }

    #[cold]
    fn serialize_u16(self, _val: u16) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received u16",
        ))
    }

    #[cold]
    fn serialize_u32(self, _val: u32) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received u32",
        ))
    }

    #[cold]
    fn serialize_u64(self, _val: u64) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received u64",
        ))
    }

    #[cold]
    fn serialize_f32(self, _val: f32) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received f32",
        ))
    }

    #[cold]
    fn serialize_f64(self, _val: f64) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received f64",
        ))
    }

    #[cold]
    fn serialize_char(self, _val: char) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received char",
        ))
    }

    #[cold]
    fn serialize_str(self, _val: &str) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received str",
        ))
    }

    #[cold]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received unit",
        ))
    }

    #[cold]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received unit_struct",
        ))
    }

    #[cold]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received unit_variant",
        ))
    }

    #[cold]
    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received newtype_struct",
        ))
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received newtype_variant",
        ))
    }

    #[cold]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received none",
        ))
    }

    #[cold]
    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received some",
        ))
    }

    #[cold]
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received seq",
        ))
    }

    #[cold]
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received tuple",
        ))
    }

    #[cold]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received tuple_struct",
        ))
    }

    #[cold]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received tuple_variant",
        ))
    }

    #[cold]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received map",
        ))
    }

    #[cold]
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received struct",
        ))
    }

    #[cold]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _idx: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(<Error as ser::Error>::custom(
            "expected i8 and bytes, received struct_variant",
        ))
    }
}

impl ExtSerializer {
    #[inline]
    fn new() -> Self {
        Self { fields_se: None }
    }

    fn value(self) -> Result<Value, Error> {
        match self.fields_se {
            Some(fields_se) => fields_se.value(),
            None => Err(<Error as ser::Error>::custom(
                "expected tuple, received nothing",
            )),
        }
    }
}

impl ExtFieldSerializer {
    #[inline]
    fn new() -> Self {
        Self {
            tag: None,
            binary: None,
        }
    }

    fn value(self) -> Result<Value, Error> {
        match (self.tag, self.binary) {
            (Some(tag), Some(binary)) => Ok(Value::Ext(tag, binary)),
            (Some(_), None) => Err(<Error as ser::Error>::custom(
                "expected i8 and bytes, received i8 only",
            )),
            (None, Some(_)) => Err(<Error as ser::Error>::custom(
                "expected i8 and bytes, received bytes only",
            )),
            (None, None) => Err(<Error as ser::Error>::custom(
                "expected i8 and bytes, received nothing",
            )),
        }
    }
}

#[doc(hidden)]
pub struct SerializeVec {
    vec: Vec<Value>,
}

/// Default implementation for tuple variant serialization. It packs given enums as a tuple of an
/// index with a tuple of arguments.
#[doc(hidden)]
pub struct SerializeTupleVariant {
    variant: &'static str,
    vec: Vec<Value>,
}

#[doc(hidden)]
pub struct DefaultSerializeMap {
    map: Vec<(Value, Value)>,
    next_key: Option<Value>,
}

#[doc(hidden)]
pub struct SerializeStructVariant {
    variant: &'static str,
    map: Vec<(Value, Value)>,
}

impl ser::SerializeSeq for SerializeVec {
    type Ok = Value;
    type Error = Error;

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.vec.push(to_rmpv_named(value)?);
        Ok(())
    }

    #[inline]
    fn end(self) -> Result<Value, Error> {
        Ok(Value::Array(self.vec))
    }
}

impl ser::SerializeTuple for SerializeVec {
    type Ok = Value;
    type Error = Error;

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    #[inline]
    fn end(self) -> Result<Value, Error> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleStruct for SerializeVec {
    type Ok = Value;
    type Error = Error;

    #[inline]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    #[inline]
    fn end(self) -> Result<Value, Error> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleVariant for SerializeTupleVariant {
    type Ok = Value;
    type Error = Error;

    #[inline]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.vec.push(to_rmpv_named(value)?);
        Ok(())
    }

    #[inline]
    fn end(self) -> Result<Value, Error> {
        Ok(Value::Map(vec![(
            Value::from(self.variant),
            Value::Array(self.vec),
        )]))
    }
}

impl ser::SerializeMap for DefaultSerializeMap {
    type Ok = Value;
    type Error = Error;

    #[inline(always)]
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.next_key = Some(to_rmpv_named(key)?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        // Panic because this indicates a bug in the program rather than an
        // expected failure.
        let key = self
            .next_key
            .take()
            .expect("`serialize_value` called before `serialize_key`");
        self.map.push((key, to_rmpv_named(value)?));
        Ok(())
    }

    #[inline(always)]
    fn end(self) -> Result<Value, Error> {
        Ok(Value::Map(self.map))
    }
}

impl ser::SerializeStruct for DefaultSerializeMap {
    type Ok = Value;
    type Error = Error;

    #[inline]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.map.push((to_rmpv_named(key)?, to_rmpv_named(value)?));
        Ok(())
    }

    #[inline(always)]
    fn end(self) -> Result<Value, Error> {
        Ok(Value::Map(self.map))
    }
}

impl ser::SerializeStructVariant for SerializeStructVariant {
    type Ok = Value;
    type Error = Error;

    #[inline]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
    where
        T: ?Sized + ser::Serialize,
    {
        self.map.push((to_rmpv_named(key)?, to_rmpv_named(value)?));
        Ok(())
    }

    #[inline(always)]
    fn end(self) -> Result<Value, Error> {
        Ok(Value::Map(vec![(
            Value::from(self.variant),
            Value::Map(self.map),
        )]))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn check_to_rmpv_named() {
        #[derive(serde::Serialize)]
        struct Struct {
            field: i32,
        }
        #[derive(serde::Serialize)]
        struct Tuple(i32, i32);
        #[derive(serde::Serialize)]
        struct NewType(i32);
        #[derive(serde::Serialize)]
        struct Unit;

        #[derive(serde::Serialize)]
        enum E {
            Struct { field: i32 },
            Tuple(i32, i32),
            NewType(i32),
            Unit,
        }

        // We do the same thing as what serde_json::to_value does

        assert_eq!(
            to_rmpv_named(&Struct { field: 1 }).unwrap(),
            Value::Map(vec![(Value::from("field"), Value::from(1))]),
        );

        assert_eq!(
            to_rmpv_named(&E::Struct { field: 2 }).unwrap(),
            Value::Map(vec![(
                Value::from("Struct"),
                Value::Map(vec![(Value::from("field"), Value::from(2))])
            )])
        );

        assert_eq!(
            to_rmpv_named(&Tuple(3, 4)).unwrap(),
            Value::Array(vec![Value::from(3), Value::from(4)])
        );

        assert_eq!(
            to_rmpv_named(&E::Tuple(5, 6)).unwrap(),
            Value::Map(vec![(
                Value::from("Tuple"),
                Value::Array(vec![Value::from(5), Value::from(6)])
            )])
        );

        assert_eq!(to_rmpv_named(&NewType(7)).unwrap(), Value::from(7));

        assert_eq!(
            to_rmpv_named(&E::NewType(8)).unwrap(),
            Value::Map(vec![(Value::from("NewType"), Value::from(8))])
        );

        assert_eq!(to_rmpv_named(&Unit).unwrap(), Value::Nil);
        assert_eq!(to_rmpv_named(&E::Unit).unwrap(), Value::from("Unit"));
    }
}
