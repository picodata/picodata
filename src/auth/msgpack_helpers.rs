/// A helper type that will successfully deserialize from both msgpack string and msgpack bytes.
pub struct StringOrBytes<'a>(pub &'a [u8]);

impl<'de> serde::Deserialize<'de> for StringOrBytes<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StringOrBytes<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a string or bytes")
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StringOrBytes(v.as_bytes()))
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StringOrBytes(v))
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}
