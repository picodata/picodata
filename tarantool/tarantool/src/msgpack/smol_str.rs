use crate::msgpack::encode::Context;
use crate::msgpack::encode::Decode;
use crate::msgpack::encode::DecodeError;
use crate::msgpack::encode::Encode;
use crate::msgpack::encode::EncodeError;
use smol_str::SmolStr;
use std::io::Write;

impl Encode for SmolStr {
    #[inline(always)]
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        rmp::encode::write_str(w, self).map_err(Into::into)
    }
}

impl<'de> Decode<'de> for SmolStr {
    #[inline]
    fn decode(r: &mut &'de [u8], _context: &Context) -> Result<Self, DecodeError> {
        let (res, bound) =
            rmp::decode::read_str_from_slice(*r).map_err(DecodeError::new::<Self>)?;
        *r = bound;
        Ok(Self::new(res))
    }
}
