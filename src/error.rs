
use std::io;
use std::fmt;

enum DbErr {
    ParseError,
    IOErr(io::Error),
}

impl fmt::Display for DbErr {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbErr::ParseError => write!(f, "ParseError"),
            DbErr::IOErr(io_err) => io_err.fmt(f)
        }
    }

}

impl From<io::Error> for DbErr {

    fn from(error: io::Error) -> Self {
        DbErr::IOErr(error)
    }

}
