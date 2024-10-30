#[derive(Debug)]
pub struct Fd(libc::c_int);

impl Fd {
    /// # Safety
    /// `Fd` takes ownership of `fd` and will be responsible for calling `close`
    /// on it, so the caller must make sure to not do so for the original `fd`.
    #[inline(always)]
    pub unsafe fn from_raw(fd: u32) -> Self {
        Self(fd as _)
    }
}

impl std::ops::Deref for Fd {
    type Target = libc::c_int;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::io::Read for Fd {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // SAFETY: safe because pointer points into a valid array
        let rc = unsafe { libc::read(self.0, buf.as_mut_ptr() as _, buf.len()) };

        if rc < 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(rc as _)
    }
}

impl std::io::Write for Fd {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // SAFETY: safe because pointer points into a valid array
        let rc = unsafe { libc::write(self.0, buf.as_ptr() as _, buf.len()) };

        if rc < 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(rc as _)
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for Fd {
    fn drop(&mut self) {
        // SAFETY: safe, because fd is owned by self, hence close is only called once
        let rc = unsafe { libc::close(self.0) };

        if let Err(e) = check_return_code(rc) {
            crate::tlog!(Error, "close failed: {e}");
        }
    }
}

pub fn pipe() -> std::io::Result<(Fd, Fd)> {
    let mut fds = [0_i32; 2];

    // SAFETY: safe because the array is big enough
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };

    check_return_code(rc)?;

    Ok((Fd(fds[0]), Fd(fds[1])))
}

pub fn check_return_code(rc: i32) -> std::io::Result<()> {
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}
