/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
use std::fmt;
use std::io::Write;
use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use std::ptr::null_mut;
use std::os::raw::c_uint;
use libc;
use super::hex;
use crate::db::DbResult;
use crate::error::{DbErr, parse_error_reason};

#[derive(Debug, Clone, Eq)]
pub struct ObjectId {
    timestamp: u64,
    counter:   u32,
}

impl ObjectId {

    pub fn deserialize(bytes: &[u8]) -> DbResult<ObjectId> {
        if bytes.len() != 12 {
            return Err(DbErr::ParseError(parse_error_reason::OBJECT_ID_LEN.into()));
        }

        let mut timestamp_buffer: [u8; 8] = [0; 8];
        timestamp_buffer.copy_from_slice(&bytes[0..8]);
        let timestamp = u64::from_be_bytes(timestamp_buffer);

        let mut counter_buffer: [u8; 4] = [0; 4];
        counter_buffer.copy_from_slice(&bytes[8..12]);
        let counter = u32::from_be_bytes(counter_buffer);

        Ok(ObjectId { timestamp, counter })
    }

    #[allow(dead_code)]
    fn from_hex(data: &str) -> DbResult<ObjectId> {
        let bytes = match hex::decode(data) {
            Ok(result) => result,
            Err(_) => return Err(DbErr::ParseError(parse_error_reason::OBJECT_ID_HEX_DECODE_ERROR.into()))
        };

        ObjectId::deserialize(&bytes)
    }

    fn to_hex(&self) -> String {
        let mut bytes = vec![];

        self.serialize(&mut bytes).expect("object id serializing failed");

        hex::encode(bytes)
    }

    pub fn serialize(&self, writer: &mut dyn Write) -> DbResult<()> {
        let timestamp_le: [u8; 8] = self.timestamp.to_be_bytes();
        let counter_le: [u8; 4] = self.counter.to_be_bytes();

        writer.write_all(&timestamp_le)?;
        writer.write_all(&counter_le)?;

        Ok(())
    }

}

impl fmt::Display for ObjectId {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_hex().as_str())
    }

}

impl Ord for ObjectId {

    fn cmp(&self, other: &Self) -> Ordering {
        let tmp = self.timestamp.cmp(&other.timestamp);
        match tmp {
            Ordering::Equal => self.counter.cmp(&other.counter),
            _  => tmp
        }
    }

}

impl PartialOrd for ObjectId {

    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }

}

impl PartialEq for ObjectId {

    fn eq(&self, other: &Self) -> bool {
        self.counter == other.counter && self.timestamp == other.timestamp
    }

}

#[derive(Debug)]
pub struct ObjectIdMaker {
    pub counter:   u32,
}

fn random_i32() -> i32 {
    unsafe {
        libc::rand()
    }
}

impl ObjectIdMaker {

    pub fn new() -> ObjectIdMaker {
        unsafe {
            let time = libc::time(null_mut());
            libc::srand(time as c_uint);
        }
        let counter: u32 = random_i32() as u32;
        return ObjectIdMaker { counter };
    }

    pub fn mk_object_id(&mut self) -> ObjectId {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let in_ms = since_the_epoch.as_secs() * 1000 +
            since_the_epoch.subsec_nanos() as u64 / 1_000_000;

        let id = self.counter;
        self.plus_counter();
        ObjectId {
            timestamp: in_ms,
            counter : id,
        }
    }

    // avoid overflow
    #[inline]
    pub fn plus_counter(&mut self) {
        if self.counter == u32::max_value() {
            self.counter = 0;
            return;
        }
        self.counter += 1;
    }

    pub fn value_of(content: &str) -> DbResult<ObjectId> {
        if content.len() != 12 {
            return Err(DbErr::ParseError(parse_error_reason::OBJECT_ID_HEX_DECODE_ERROR.into()));
        }

        let timestamp_str = &content[0..8];
        let counter_str = &content[8..12];

        let timestamp: u64 = timestamp_str.parse::<u64>()?;
        let counter: u32 = counter_str.parse::<u32>()?;

        Ok(ObjectId {
            timestamp,
            counter,
        })
    }

}

#[cfg(test)]
mod tests {
    use crate::bson::object_id::{ ObjectIdMaker, ObjectId };

    #[test]
    fn object_id_not_zero() {
        let mut maker = ObjectIdMaker::new();
        let oid = maker.mk_object_id();

        assert_ne!(oid.timestamp, 0);
    }

    #[test]
    fn object_to_hex() {
        let mut maker = ObjectIdMaker::new();
        let oid = maker.mk_object_id();

        let hex_str = oid.to_hex();
        let from_hex = ObjectId::from_hex(hex_str.as_str()).expect("parse error");

        assert_eq!(from_hex, oid)
    }

}
