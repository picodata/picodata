//! Double type module.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::num::NonZeroI32;
use std::str::FromStr;

use crate::errors::{Entity, SbroadError};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use tarantool::decimal::Decimal;
use tarantool::msgpack::{Context, Decode, DecodeError, Encode, EncodeError};
use tarantool::tlua;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(transparent)]
pub struct Double {
    pub value: f64,
}

impl Eq for Double {}

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Double {
    /// `PostgreSQL`: <https://github.com/postgres/postgres/blob/ae0e1be9f2a20f6b64072dcee5b8dd7b9027a8fa/src/backend/utils/adt/numeric.c#L2522>
    fn cmp(&self, other: &Self) -> Ordering {
        let self_value = self.value;
        let other_value = other.value;
        let is_special = |f: f64| f.is_nan() || f.is_infinite();
        let self_is_special = is_special(self_value);
        let other_is_special = is_special(other_value);

        if self_is_special {
            if self_value.is_nan() {
                if other_value.is_nan() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            } else if self_value == f64::INFINITY {
                if other_value.is_nan() {
                    Ordering::Less
                } else if other_value == f64::INFINITY {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            } else if other_value == f64::NEG_INFINITY {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        } else if other_is_special {
            if other_value == f64::NEG_INFINITY {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        } else {
            let self_decimal = Decimal::try_from(self_value).unwrap();
            let other_decimal = Decimal::try_from(other_value).unwrap();
            self_decimal.cmp(&other_decimal)
        }
    }
}

impl<'de> Decode<'de> for Double {
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        let v = f64::decode(r, context)?;

        Ok(Self::from(v))
    }
}

impl Encode for Double {
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        self.value.encode(w, context)
    }
}

#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for Double {
    /// We get hash from the internal float64 bit representation.
    /// As a side effect, `hash(NaN) == hash(NaN)` is true. We
    /// should manually care about this case in the code.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.to_bits().hash(state);
    }
}

impl fmt::Display for Double {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl From<f64> for Double {
    fn from(f: f64) -> Self {
        Double { value: f }
    }
}

#[allow(clippy::cast_precision_loss)]
impl From<i64> for Double {
    fn from(i: i64) -> Self {
        Double { value: i as f64 }
    }
}

#[allow(clippy::cast_precision_loss)]
impl From<u64> for Double {
    fn from(u: u64) -> Self {
        Double { value: u as f64 }
    }
}

impl FromStr for Double {
    type Err = SbroadError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Double {
            value: s.parse::<f64>().map_err(|_| {
                SbroadError::ParsingError(Entity::Value, format_smolstr!("{s} is not a valid f64"))
            })?,
        })
    }
}

impl<L: tlua::AsLua> tlua::Push<L> for Double {
    type Err = tlua::Void;

    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        self.value.push_to_lua(lua)
    }
}

impl<L> tlua::PushInto<L> for Double
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;
    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
        self.value.push_into_lua(lua)
    }
}

impl<L> tlua::PushOneInto<L> for Double where L: tlua::AsLua {}

impl<L> tlua::LuaRead<L> for Double
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Double, (L, tlua::WrongType)> {
        let val: Result<tlua::UserdataOnStack<f64, _>, _> =
            tlua::LuaRead::lua_read_at_position(lua, index);
        match val {
            Ok(d) => Ok(Double { value: *d }),
            Err(lua) => Err(lua),
        }
    }
}

#[cfg(test)]
mod tests;
