use serde::{Deserialize, Serialize};

#[cfg(not(feature = "picodata"))]
crate::define_str_enum! {
    #[derive(Default)]
    pub enum AuthMethod {
        #[default]
        ChapSha1 = "chap-sha1",
    }
}

#[cfg(feature = "picodata")]
crate::define_str_enum! {
    #[derive(Default, clap::ArgEnum)]
    pub enum AuthMethod {
        #[default]
        ChapSha1 = "chap-sha1",
        Md5 = "md5",
        Ldap = "ldap",
    }
}

#[cfg(feature = "picodata")]
mod picodata {
    use super::AuthMethod;
    use crate::ffi::tarantool as ffi;
    use std::mem::MaybeUninit;
    use std::ops::Range;

    pub(super) fn auth_data_prepare(method: &AuthMethod, user: &str, password: &str) -> String {
        let Range {
            start: pwd_start,
            end: pwd_end,
        } = password.as_bytes().as_ptr_range();
        let Range {
            start: user_start,
            end: user_end,
        } = user.as_bytes().as_ptr_range();
        let Range {
            start: auth_start,
            end: auth_end,
        } = method.as_str().as_bytes().as_ptr_range();
        let mut data = MaybeUninit::uninit();
        let mut data_end = MaybeUninit::uninit();
        let svp = unsafe { ffi::box_region_used() };
        unsafe {
            ffi::box_auth_data_prepare(
                auth_start as _,
                auth_end as _,
                pwd_start as _,
                pwd_end as _,
                user_start as _,
                user_end as _,
                data.as_mut_ptr(),
                data_end.as_mut_ptr(),
            );
        }
        let data = unsafe { data.assume_init() };
        let data_end = unsafe { data_end.assume_init() };
        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(data as *const u8, data_end.offset_from(data) as usize)
        };
        let auth_data = rmp_serde::from_slice(bytes).unwrap();
        unsafe { ffi::box_region_truncate(svp) };
        auth_data
    }
}

pub struct AuthData(String);

impl AuthData {
    #[cfg(feature = "picodata")]
    pub fn new(method: &AuthMethod, user: &str, password: &str) -> Self {
        let data = picodata::auth_data_prepare(method, user, password);
        Self(data)
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthDef {
    pub method: AuthMethod,
    /// Base64 encoded digest.
    pub data: String,
}

impl AuthDef {
    pub fn new(method: AuthMethod, data: String) -> Self {
        Self { method, data }
    }
}
