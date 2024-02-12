use std::io::{Cursor, Write};

use crate::error::Error;
use crate::tuple::{ToTupleBuffer, Tuple};

use super::codec::IProtoType;
use super::{codec, SyncIndex};

pub trait Request {
    const TYPE: IProtoType;
    type Response: Sized;

    #[inline(always)]
    fn encode_header(&self, out: &mut impl Write, sync: SyncIndex) -> Result<(), Error> {
        codec::encode_header(out, sync, Self::TYPE)
    }

    fn encode_body(&self, out: &mut impl Write) -> Result<(), Error>;

    fn encode(&self, out: &mut impl Write, sync: SyncIndex) -> Result<(), Error> {
        self.encode_header(out, sync)?;
        self.encode_body(out)?;
        Ok(())
    }

    fn decode_response_body(r#in: &mut Cursor<Vec<u8>>) -> Result<Self::Response, Error>;
}

// TODO: Implement `Request` for other types in `IProtoType`

pub struct Ping;

impl Request for Ping {
    const TYPE: IProtoType = IProtoType::Ping;
    type Response = ();

    #[inline(always)]
    fn encode_body(&self, out: &mut impl Write) -> Result<(), Error> {
        codec::encode_ping(out)
    }

    #[inline(always)]
    fn decode_response_body(_in: &mut Cursor<Vec<u8>>) -> Result<Self::Response, Error> {
        Ok(())
    }
}

pub struct Call<'a, 'b, T: ?Sized> {
    pub fn_name: &'a str,
    pub args: &'b T,
}

impl<'a, 'b, T> Request for Call<'a, 'b, T>
where
    T: ToTupleBuffer + ?Sized,
{
    const TYPE: IProtoType = IProtoType::Call;
    type Response = Tuple;

    #[inline(always)]
    fn encode_body(&self, out: &mut impl Write) -> Result<(), Error> {
        codec::encode_call(out, self.fn_name, self.args)
    }

    #[inline(always)]
    fn decode_response_body(r#in: &mut Cursor<Vec<u8>>) -> Result<Self::Response, Error> {
        codec::decode_call(r#in)
    }
}

pub struct Eval<'a, 'b, T: ?Sized> {
    pub expr: &'a str,
    pub args: &'b T,
}

impl<'a, 'b, T> Request for Eval<'a, 'b, T>
where
    T: ToTupleBuffer + ?Sized,
{
    const TYPE: IProtoType = IProtoType::Eval;
    type Response = Tuple;

    #[inline(always)]
    fn encode_body(&self, out: &mut impl Write) -> Result<(), Error> {
        codec::encode_eval(out, self.expr, self.args)
    }

    #[inline(always)]
    fn decode_response_body(r#in: &mut Cursor<Vec<u8>>) -> Result<Self::Response, Error> {
        codec::decode_call(r#in)
    }
}

pub struct Execute<'a, 'b, T: ?Sized> {
    pub sql: &'a str,
    pub bind_params: &'b T,
}

impl<'a, 'b, T> Request for Execute<'a, 'b, T>
where
    T: ToTupleBuffer + ?Sized,
{
    const TYPE: IProtoType = IProtoType::Execute;
    type Response = Vec<Tuple>;

    #[inline(always)]
    fn encode_body(&self, out: &mut impl Write) -> Result<(), Error> {
        codec::encode_execute(out, self.sql, self.bind_params)
    }

    #[inline(always)]
    fn decode_response_body(r#in: &mut Cursor<Vec<u8>>) -> Result<Self::Response, Error> {
        codec::decode_multiple_rows(r#in)
    }
}

pub struct Auth<'u, 'p, 's> {
    pub user: &'u str,
    pub pass: &'p str,
    pub salt: &'s [u8],
    pub method: crate::auth::AuthMethod,
}

impl<'u, 'p, 's> Request for Auth<'u, 'p, 's> {
    const TYPE: IProtoType = IProtoType::Auth;
    type Response = ();

    #[inline(always)]
    fn encode_body(&self, out: &mut impl Write) -> Result<(), Error> {
        codec::encode_auth(out, self.user, self.pass, self.salt, self.method)
    }

    #[inline(always)]
    fn decode_response_body(_in: &mut Cursor<Vec<u8>>) -> Result<Self::Response, Error> {
        Ok(())
    }
}
