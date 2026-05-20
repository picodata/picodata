#![cfg(feature = "picodata")]

use tarantool::auth::{AuthData, AuthMethod};

#[tarantool::test]
pub fn chap_sha1() {
    let data = AuthData::new(&AuthMethod::ChapSha1, "", "password");
    assert_eq!(&data.into_string(), "JHDAwG3uQv0WGLuZAFrcouydHhk=");
}
