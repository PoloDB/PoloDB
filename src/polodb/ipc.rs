use std::path::Path;
use std::io::{Result, Read, Write};
#[cfg(windows)]
use std::ptr::null_mut;
#[cfg(windows)]
use winapi::um::namedpipeapi::{CreateNamedPipeW, DisconnectNamedPipe, ConnectNamedPipe};
#[cfg(windows)]
use winapi::um::winnt::HANDLE;
#[cfg(windows)]
use winapi::um::winbase::{PIPE_ACCESS_DUPLEX, FILE_FLAG_FIRST_PIPE_INSTANCE,
    PIPE_TYPE_BYTE, PIPE_WAIT, PIPE_REJECT_REMOTE_CLIENTS, PIPE_UNLIMITED_INSTANCES};
#[cfg(windows)]
use winapi::um::handleapi::INVALID_HANDLE_VALUE;
#[cfg(windows)]
use winapi::um::errhandlingapi::GetLastError;
#[cfg(windows)]
use std::marker::PhantomData;

#[cfg(unix)]
use std::os::unix::net::{UnixStream, UnixListener};

#[cfg(unix)]
pub struct IPC {
    server: UnixListener,
}

#[cfg(unix)]
pub struct Connection {
    socket: UnixStream,
}

#[cfg(unix)]
pub struct Incoming<'a> {
    inner: std::os::unix::net::Incoming<'a>,
}

#[cfg(unix)]
impl IPC {

    pub fn bind<P: AsRef<Path>>(p: P) -> Result<IPC> {
        let server = UnixListener::bind(p)?;
        Ok(IPC {
            server
        })
    }

    pub fn incoming(&self) -> Incoming<'_> {
        let inner = self.server.incoming();
        Incoming { inner }
    }

}

#[cfg(unix)]
impl<'a> Iterator for Incoming<'a> {
    type Item = std::io::Result<Connection>;

    fn next(&mut self) -> Option<Result<Connection>> {
        self.inner.next().map(|result| {
            result.map(|s| {
                Connection { socket: s }
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (usize::MAX, None)
    }
}

#[cfg(unix)]
impl Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.socket.read(buf)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        self.socket.read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
        self.socket.read_to_string(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.socket.read_exact(buf)
    }

    fn by_ref(&mut self) -> &mut Self where Self: Sized {
        self
    }

}

#[cfg(unix)]
impl Write for Connection {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.socket.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.socket.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.socket.write_all(buf)
    }
}

#[cfg(windows)]
pub struct IPC {
    pipe: HANDLE,
}

#[cfg(windows)]
pub struct Connection {
    conn: HANDLE,
}

#[cfg(windows)]
pub struct Incoming<'a> {
    inner: HANDLE,
    phantom: PhantomData<&'a HANDLE>,
}

#[cfg(windows)]
impl<'a> Iterator for Incoming<'a> {
    type Item = std::io::Result<Connection>;

    fn next(&mut self) -> Option<Result<Connection>> {
        let connected = unsafe { ConnectNamedPipe(self.inner, null_mut()) };
        if connected == 0 {
            let err_kind = std::io::ErrorKind::ConnectionRefused;
            let err_code = unsafe { GetLastError() };
            let custom_error = std::io::Error::new(err_kind, format!("{}", err_code));
            return Some(Err(custom_error));
        }
        let conn = Connection {
            conn: self.inner,
        };
        Some(Ok(conn))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (usize::MAX, None)
    }
}

#[cfg(windows)]
impl IPC {

    pub fn bind<P: AsRef<Path>>(p: P) -> Result<IPC> {
        let path = p.as_ref();
        let path = path.to_str().unwrap();
        let wpath: Vec<u16> = path.encode_utf16().collect();
        let handle = unsafe {
            CreateNamedPipeW(
                wpath.as_ptr(),
                PIPE_ACCESS_DUPLEX | FILE_FLAG_FIRST_PIPE_INSTANCE,
                PIPE_TYPE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                PIPE_UNLIMITED_INSTANCES,
                4096,
                4096,
                0,
                null_mut())
        };

        if handle == INVALID_HANDLE_VALUE {
            let err_kind = std::io::ErrorKind::ConnectionRefused;
            let err_code = unsafe { GetLastError() };
            let custom_error = std::io::Error::new(err_kind, format!("{}", err_code));
            return Err(custom_error);
        }

        Ok(IPC {
            pipe: handle as HANDLE
        })
    }

    pub fn incoming(&self) -> Incoming<'_> {
        Incoming {
            inner: self.pipe,
            phantom: PhantomData,
        }
    }

}

#[cfg(windows)]
impl Drop for IPC {

    fn drop(&mut self) {
        unsafe {
            DisconnectNamedPipe(self.pipe);
        }
    }

}
