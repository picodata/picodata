#![cfg(feature = "picodata")]

use tarantool::auth::{AuthData, AuthMethod};

#[tarantool::test]
pub fn md5() {
    let data = AuthData::new(&AuthMethod::Md5, "user", "password");
    assert_eq!(&data.into_string(), "md54d45974e13472b5a0be3533de4666414");
}

#[tarantool::test]
pub fn chap_sha1() {
    let data = AuthData::new(&AuthMethod::ChapSha1, "", "password");
    assert_eq!(&data.into_string(), "JHDAwG3uQv0WGLuZAFrcouydHhk=");
}

#[tarantool::test]
pub fn ldap() {
    let data = AuthData::new(&AuthMethod::Ldap, "", "");
    assert_eq!(&data.into_string(), "");
}
