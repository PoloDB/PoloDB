/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
#[cfg(unix)]
use std::path::Path;
use std::io::{Result, Read, Write};
// use std::net::Shutdown;
#[cfg(windows)]
use std::ptr::null_mut;
#[cfg(windows)]
use winapi::um::namedpipeapi::{CreateNamedPipeW, DisconnectNamedPipe, ConnectNamedPipe};
#[cfg(windows)]
use winapi::um::winnt::HANDLE;
#[cfg(windows)]
use winapi::um::winbase::{PIPE_ACCESS_DUPLEX, PIPE_TYPE_BYTE, PIPE_WAIT,
    PIPE_REJECT_REMOTE_CLIENTS, PIPE_UNLIMITED_INSTANCES};
#[cfg(windows)]
use winapi::um::handleapi::INVALID_HANDLE_VALUE;
#[cfg(windows)]
use winapi::um::errhandlingapi::GetLastError;
#[cfg(windows)]
use winapi::um::fileapi::{ReadFile, WriteFile, FlushFileBuffers};
#[cfg(windows)]
use winapi::shared::minwindef::DWORD;
#[cfg(windows)]
use winapi::um::handleapi::CloseHandle;

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

// #[cfg(unix)]
// impl Connection {
//
//     fn shutdown(&self) -> Result<()> {
//         self.socket.shutdown(Shutdown::Both)
//     }
//
// }

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
    path: String,
    // pipe: HANDLE,
}

#[cfg(windows)]
pub struct Connection {
    handle: HANDLE,
}

#[cfg(windows)]
unsafe impl Send for Connection {}


#[cfg(windows)]
impl Connection {

    fn new(handle: HANDLE) -> Connection {
        Connection { handle }
    }

    fn connect(&mut self) -> std::io::Result<()> {
        let connected = unsafe { ConnectNamedPipe(self.handle, null_mut()) };
        if connected == 0 {
            let err_kind = std::io::ErrorKind::ConnectionRefused;
            let err_code = unsafe { GetLastError() };
            let custom_error = std::io::Error::new(err_kind, format!("{}", err_code));
            return Err(custom_error);
        }
        Ok(())
    }

}

#[cfg(windows)]
impl Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes_to_read: DWORD = buf.len() as DWORD;
        unsafe {
            let mut bytes_read: DWORD = 0;
            let bytes_read_ptr: *mut DWORD = &mut bytes_read;
            let success = ReadFile(
                self.handle,
                buf.as_mut_ptr().cast::<winapi::ctypes::c_void>(),
                bytes_to_read,
                bytes_read_ptr,
                null_mut()
            );

            if success == 0 {
                let err_kind = std::io::ErrorKind::BrokenPipe;
                let err_code = GetLastError();
                let custom_error = std::io::Error::new(err_kind, format!("{}", err_code));
                return Err(custom_error);
            }

            return Ok(bytes_read as usize)
        }
    }

    // fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
    //     self.socket.read_to_end(buf)
    // }

    // fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
    //     self.socket.read_to_string(buf)
    // }

    // fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
    //     self.socket.read_exact(buf)
    // }

    fn by_ref(&mut self) -> &mut Self where Self: Sized {
        self
    }

}

#[cfg(windows)]
impl Write for Connection {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let bytes_to_write = buf.len() as DWORD;
        unsafe {
            let mut bytes_written: DWORD = 0;
            let bytes_written_ptr: *mut DWORD = &mut bytes_written;
            let ec = WriteFile(
                self.handle,
                buf.as_ptr().cast::<winapi::ctypes::c_void>(),
                bytes_to_write,
                bytes_written_ptr,
                null_mut()
            );
            if ec == 0 {
                let err_kind = std::io::ErrorKind::BrokenPipe;
                let err_code = GetLastError();
                let custom_error = std::io::Error::new(err_kind, format!("{}", err_code));
                return Err(custom_error);
            }
            Ok(bytes_written as usize)
        }
    }

    fn flush(&mut self) -> Result<()> {
        unsafe {
            let ec = FlushFileBuffers(self.handle);
            if ec == 0 {
                let err_kind = std::io::ErrorKind::BrokenPipe;
                let err_code = GetLastError();
                let custom_error = std::io::Error::new(err_kind, format!("{}", err_code));
                return Err(custom_error);
            }
        }
        Ok(())
    }

    // fn write_all(&mut self, buf: &[u8]) -> Result<()> {
    //     self.socket.write_all(buf)
    // }
}

#[cfg(windows)]
impl Drop for Connection {

    fn drop(&mut self) {
        unsafe {
            DisconnectNamedPipe(self.handle);
            CloseHandle(self.handle);
        }
    }

}

#[cfg(windows)]
pub struct Incoming<'a> {
    path: &'a str,
}

#[cfg(windows)]
impl<'a> Iterator for Incoming<'a> {
    type Item = std::io::Result<Connection>;

    fn next(&mut self) -> Option<Result<Connection>> {
        let wpath: Vec<u16> = self.path.encode_utf16().collect();
        let handle = unsafe {
            CreateNamedPipeW(
                wpath.as_ptr(),
                PIPE_ACCESS_DUPLEX,
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
            return Some(Err(custom_error));
        }

        let mut connection = Connection::new(handle);
        if let Err(err) = connection.connect() {
            return Some(Err(err));
        }

        Some(Ok(connection))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (usize::MAX, None)
    }
}

#[cfg(windows)]
impl IPC {

    pub fn bind(path: &str) -> Result<IPC> {

        Ok(IPC {
            path: path.into(),
            // pipe: handle as HANDLE
        })
    }

    pub fn incoming(&self) -> Incoming<'_> {
        Incoming {
            path: &self.path
        }
    }

}
