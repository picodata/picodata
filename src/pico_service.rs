use crate::tlog;
use crate::traft::error::Error;
use crate::unwrap_ok_or;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

/// Password of the special system user "pico_service".
///
/// It is stored in a global variable, because we need to access it from
/// different places in code when creating iproto connections to other instances.
// TODO: for chap-sha authentication method we only need to store the sha1 hash
// of the password, but our iproto clients don't yet support this and also sha1
// is not a secure hash anyway, so ...
static mut PICO_SERVICE_PASSWORD: Option<String> = None;

#[inline(always)]
pub(crate) fn pico_service_password() -> &'static str {
    unsafe { PICO_SERVICE_PASSWORD.as_deref() }.unwrap_or("")
}

pub(crate) fn read_pico_service_password_from_file(
    filename: impl AsRef<Path>,
) -> Result<(), Error> {
    let res = read_file_contents_and_mode(filename.as_ref());
    let (data, mode) = unwrap_ok_or!(
        res,
        Err(e) => {
            return Err(Error::other(format!("failed to read password from file '{}': {e}", filename.as_ref().display())));
        }
    );

    #[rustfmt::skip]
    if (mode & 0o000_077) != 0 {
        tlog!(Warning, "*****************************************************************************");
        tlog!(Warning, "! service password file's permissions are too open, this is a security risk !");
        tlog!(Warning, "*****************************************************************************");
    };

    let Ok(text) = String::from_utf8(data) else {
        return Err(Error::other("password must be encoded as utf-8"));
    };

    let Some((password, _)) = text.split_once('\n') else {
        return Err(Error::other("service password cannot be empty"));
    };

    unsafe {
        PICO_SERVICE_PASSWORD = Some(password.into());
    }

    Ok(())
}

fn read_file_contents_and_mode(filename: impl AsRef<Path>) -> std::io::Result<(Vec<u8>, u32)> {
    let mut file = File::open(filename)?;
    let metadata = file.metadata()?;
    let mode = metadata.permissions().mode();

    let mut data = vec![];
    file.read_to_end(&mut data)?;

    Ok((data, mode))
}
