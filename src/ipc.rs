use crate::tlog;
use nix::unistd;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct Fd(pub libc::c_int);

#[derive(Debug)]
pub struct Sender<T> {
    fd: Fd,
    phantom: PhantomData<T>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    fd: Fd,
    phantom: PhantomData<T>,
}

impl<T> Sender<T>
where
    T: Serialize,
{
    pub fn send(mut self, msg: &T) {
        if let Err(e) = rmp_serde::encode::write(&mut self.fd, msg) {
            tlog!(Error, "ipc error: {e}")
        }
    }
}

impl<T> Receiver<T>
where
    T: DeserializeOwned,
{
    pub fn recv(self) -> Result<T, rmp_serde::decode::Error> {
        rmp_serde::from_read(self.fd)
    }
}

impl std::ops::Deref for Fd {
    type Target = libc::c_int;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::io::Read for Fd {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let ret = nix::unistd::read(self.0, buf)?;
        Ok(ret)
    }
}

impl std::io::Write for Fd {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(unistd::write(self.0, buf)?)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for Fd {
    fn drop(&mut self) {
        unistd::close(self.0).ok();
    }
}

pub fn channel<T>() -> Result<(Receiver<T>, Sender<T>), nix::Error>
where
    T: Serialize + DeserializeOwned,
{
    let (rx, tx) = unistd::pipe()?;
    Ok((
        Receiver {
            fd: Fd(rx),
            phantom: PhantomData,
        },
        Sender {
            fd: Fd(tx),
            phantom: PhantomData,
        },
    ))
}

pub fn pipe() -> Result<(Fd, Fd), nix::Error> {
    let (rx, tx) = unistd::pipe()?;
    Ok((Fd(rx), Fd(tx)))
}
