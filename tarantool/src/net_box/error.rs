#[derive(Debug, Fail)]
pub enum NetBoxError {
    #[fail(display = "Bad response")]
    BadResponse,

    #[fail(display = "{}", _0)]
    ResponseMessage(String),
}
