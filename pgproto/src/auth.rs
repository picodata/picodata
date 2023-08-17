use std::os::raw::c_int;

extern "C" {
    /// pointers must have valid and non-null values, salt must be at least 20 bytes
    #[link_name = "authenticate"]
    fn authenticate_raw(
        user: *const u8,
        user_len: u32,
        salt: *const u8,
        auth_info: *const u8,
    ) -> c_int;
}

/// Build auth_info that is used by authenticate_raw.
fn build_auth_info(client_pass: &str) -> Vec<u8> {
    let auth_packet = ["md5", client_pass];

    // mp_sizeof(["md5", md5-hash]) == 42,
    // but it may vary if we get a non md5 password.
    let mut result = Vec::with_capacity(42);
    rmp_serde::encode::write(&mut result, &auth_packet).unwrap();

    result
}

/// Perform authentication.
/// In case of success true is returned, false otherwise.
pub fn authenticate(user: &str, salt: &[u8; 20], client_pass: &str) -> bool {
    let auth_info = build_auth_info(client_pass);
    // SAFETY: pointers must have valid and non-null values, salt must be at least 20 bytes
    let ret = unsafe {
        authenticate_raw(
            user.as_ptr(),
            user.len() as u32,
            salt.as_ptr(),
            auth_info.as_ptr(),
        )
    };
    ret == 0
}
