use std::path::Path;
use std::io::{Result, Read, Write};
use std::os::unix::net::{UnixStream, UnixListener};

pub struct IPC {
    server: UnixListener,
}

pub struct Connection {
    socket: UnixStream,
}

pub struct Incoming<'a> {
    inner: std::os::unix::net::Incoming<'a>,
}

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
