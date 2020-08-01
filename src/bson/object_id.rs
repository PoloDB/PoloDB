use std::time::Instant;
use std::fmt;
use std::num::ParseIntError;

#[derive(Debug)]
pub struct ObjectId {
    pub timestamp: i32,
    pub counter:   i64,
}

#[derive(Debug)]
pub struct ObjectIdMaker {
    pub counter:   i64,
}

#[derive(Debug, Clone)]
pub enum ParseError {
    ParseInt(ParseIntError),
    Length(),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::ParseInt(parse_int_err) => parse_int_err.fmt(f),
            ParseError::Length() =>
                write!(f, "the length of str should be 12")
        }
    }
}

impl std::error::Error for ParseError{}

impl From<ParseIntError> for ParseError {
    fn from(err: ParseIntError) -> ParseError {
        return ParseError::ParseInt(err);
    }
}


impl ObjectIdMaker {

    pub fn new() -> ObjectIdMaker {
        return ObjectIdMaker { counter: 0 };
    }

    pub fn mk_object_id(&mut self) -> ObjectId {
        let start = Instant::now();

        let elapsed = start.elapsed();

        let id = self.counter;
        self.counter += 1;
        ObjectId {
            timestamp: elapsed.as_millis() as i32,
            counter : id,
        }
    }

    pub fn value_of(content: &str) -> Result<ObjectId, ParseError> {
        if content.len() != 12 {
            return Err(ParseError::Length())
        }

        let timestamp_str = &content[0..4];
        let counter_str = &content[4..12];

        let timestamp: i32 = timestamp_str.parse::<i32>()?;
        let counter: i64 = counter_str.parse::<i64>()?;

        Ok(ObjectId {
            timestamp,
            counter,
        })
    }

}
