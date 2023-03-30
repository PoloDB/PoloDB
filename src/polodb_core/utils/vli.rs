/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use bson::ser::Result as BsonResult;
use bson::ser::Error as BsonErr;
use std::io::{Read, Write};
use byteorder::ReadBytesExt;

// Extended from http://www.dlugosz.com/ZIP2/VLI.html

// prefix bits	bytes	data bits	unsigned range
// 0	        1	    7	        127
// 10	        2	    14	        16,383
// 110	        3	    21	        2,097,151
// 111 00	    4	    27	        134,217,727 (128K)
// 111 01	    5	    35	        34,359,738,368 (32G)
// 111 10	    8	    59	        holds the significant part of a Win32 FILETIME
// 111 11 000	6	    40	        1,099,511,627,776 (1T)
// 111 11 001	9	    64	        A full 64-bit value with one byte overhead
// 111 11 010	17	    128	        A GUID/UUID
// 111 11 011	n	    any	        A negative number
// 111 11 111	n	    any	        Any multi-precision integer

const BYTE_MARK1: u8 = 0b10000000;
const BYTE_MARK2: u8 = 0b11000000;
const BYTE_MARK3: u8 = 0b11100000;
const BYTE_MARK5: u8 = 0b11111000;
const NEG_FLAG:   u8 = 0b11111011;

pub fn encode<W: Write>(writer: &mut W, num: i64) -> BsonResult<()> {
    if num < 0 {
        writer.write_all(&[ NEG_FLAG ])?;
        return encode_u64(writer, (num * -1) as u64);
    }
    encode_u64(writer, num as u64)
}

#[inline]
fn encode_u64<W: Write>(writer: &mut W, num: u64) -> BsonResult<()> {
    if num <= 127 {
        writer.write_all(&[ (num as u8) ])?;
    } else if num <= 16383 {  // 2 bytes
        let num: u64 = 0b10000000 << 8 | num;
        writer.write_all(num.to_be_bytes()[6..8].as_ref())?;
    } else if num <= 2097151 {  // 3 bytes
        let num: u64 = 0b11000000 << 16 | num;
        writer.write_all(num.to_be_bytes()[5..8].as_ref())?;
    } else if num <= 134217727 {  // 4 bytes
        let num: u64 = 0b11100000 << 24 | num;
        writer.write_all(num.to_be_bytes()[4..8].as_ref())?;
    } else if num <= 34359738367 {  // 5 bytes
        let num: u64 = 0b11101000 << 32 | num;
        writer.write_all(num.to_be_bytes()[3..8].as_ref())?;
    } else if num <= 0xFFFFFFFFFF {  // 6 bytes
        let num: u64 = 0b11111000 << 40 | num;
        writer.write_all(num.to_be_bytes()[2..8].as_ref())?;
    } else if num <= 0xFFFFFFFFFFFFFFF { // 8 bytes
        let num: u64 = 0b11110000 << 56 | num;
        writer.write_all(num.to_be_bytes()[0..8].as_ref())?;
    } else {  // 9 bytes
        writer.write_all(&[ 0b11111001 ])?;
        let tmp = num.to_be_bytes();
        writer.write_all(&tmp)?;
    }

    Ok(())
}

#[allow(dead_code)]
pub fn vli_len(num: i64) -> usize {
    if num < 0 {
        return vli_len_u64((num * -1) as u64) + 1;
    }
    vli_len_u64(num as u64)
}

pub fn vli_len_u64(num: u64) -> usize {
    if num <= 127 {
        1
    } else if num <= 16383 {  // 2 bytes
        2
    } else if num <= 2097151 {  // 3 bytes
        3
    } else if num <= 134217727 {  // 4 bytes
        4
    } else if num <= 34359738367 {  // 5 bytes
        5
    } else if num <= 0xFFFFFFFFFF {  // 6 bytes
        6
    } else if num <= 0xFFFFFFFFFFFFFFF { // 8 bytes
        8
    } else {  // 9 bytes
        9
    }
}

#[allow(dead_code)]
pub fn decode<R: Read>(reader: &mut R) -> BsonResult<i64> {
    let first_byte = reader.read_u8()?;
    if first_byte == NEG_FLAG {
        let tmp = decode_u64(reader)?;
        return Ok((tmp as i64) * -1)
    }
    let tmp = decode_u64_with_first_byte(reader, first_byte)?;
    Ok(tmp as i64)
}

pub fn decode_u64<R: Read>(reader: &mut R) -> BsonResult<u64> {
    let first_byte = reader.read_u8()?;
    decode_u64_with_first_byte(reader, first_byte)
}

fn decode_u64_with_first_byte<R: Read>(reader: &mut R, first_byte: u8) -> BsonResult<u64> {
    if (first_byte & BYTE_MARK1) == 0 {  // 1 byte
        return Ok(first_byte as u64)
    }

    if first_byte & BYTE_MARK2 == 0b10000000 {  // 2 bytes
        let one_more = reader.read_u8()?;

        let uint16: u16 = u16::from_be_bytes([
            first_byte & (!BYTE_MARK1), one_more
        ]);
        return Ok(uint16 as u64)
    }

    if first_byte & BYTE_MARK3 == 0b11000000 {  // 3 bytes
        let mut tmp: [u8; 4] = [0; 4];
        // iter.next

        // tmp[0] is 0
        tmp[1] = first_byte & (!BYTE_MARK3);
        tmp[2] = reader.read_u8()?;
        tmp[3] = reader.read_u8()?;

        return Ok(u32::from_be_bytes(tmp) as u64)
    }

    match first_byte & BYTE_MARK5 {  // three arms
        0b11100000 => {  // 4 bytes
            let mut tmp: [u8; 4] = [0; 4];

            tmp[0] = first_byte & (!BYTE_MARK5);
            tmp[1] = reader.read_u8()?;
            tmp[2] = reader.read_u8()?;
            tmp[3] = reader.read_u8()?;

            return Ok(u32::from_be_bytes(tmp) as u64)
        }

        0b11101000 => {  // 5 bytes
            let mut tmp: [u8; 8] = [0; 8];

            tmp[3] = first_byte & (!BYTE_MARK5);
            for i in 4..8 {
                tmp[i] = reader.read_u8()?;
            }

            return Ok(u64::from_be_bytes(tmp))
        }

        0b11110000 => {  // 8 bytes
            let mut tmp: [u8; 8] = [0; 8];

            tmp[0] = first_byte & (!BYTE_MARK5);
            for i in 1..8 {
                tmp[i] = reader.read_u8()?;
            }

            return Ok(u64::from_be_bytes(tmp))
        }

        _ => ()
    }

    if first_byte == 0b11111000 {  // 6 bytes
        let mut tmp: [u8; 8] = [0; 8];
        for i in 3..8 {
            tmp[i] = reader.read_u8()?;
        }

        return Ok(u64::from_be_bytes(tmp));
    }

    if first_byte == 0b11111001 {  // 9 bytes
        let mut tmp: [u8; 8] = [0; 8];
        for i in 0..8 {
            tmp[i] = reader.read_u8()?;
        }

        return Ok(u64::from_be_bytes(tmp));
    }

    Err(BsonErr::InvalidCString("DecodeIntUnknownByte".to_string()))
}

#[cfg(test)]
mod tests {
    use crate::utils::vli::{encode_u64, decode_u64, encode, decode, vli_len_u64, vli_len};

    #[test]
    fn test_legacy_negative() {
        let n1: Vec<u8> = vec![0xf9, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        let mut n: &[u8] = n1.as_ref();
        assert_eq!(decode_u64(&mut n).unwrap() as i64, -1);
        let mut n: &[u8] = n1.as_ref();
        assert_eq!(decode(&mut n).unwrap(), -1);
    }

    #[test]
    fn test_new_negative() {
        let mut bytes = vec![];
        encode(&mut bytes, -1).expect("encode error");
        assert_eq!(bytes.len(), 2);
        assert_eq!(vli_len(-1), 2);

        for i in -20000..-1 {
            let num = i as i64;
            let mut bytes = vec![];
            encode(&mut bytes, num).expect("encode error");

            let mut bytes_ref: &[u8] = bytes.as_ref();

            assert_eq!(decode(&mut bytes_ref).unwrap(), num);
        }
    }

    #[test]
    fn test_ts() {
        let mut bytes = vec![];
        let num: u64 = 1_606_801_056_488;
        encode_u64(&mut bytes, num).expect("encode error");

        let mut bytes_ref: &[u8] = bytes.as_ref();
        let decode_num = decode_u64(&mut bytes_ref).unwrap();
        assert_eq!(decode_num, num);
    }

    #[test]
    fn test_encode_1byte() {
        let mut bytes = vec![];

        encode_u64(&mut bytes, 123).expect("encode error");

        assert_eq!(bytes[0], 123);
    }

    #[test]
    fn test_encode_2bytes() {
        let mut bytes = vec![];

        encode_u64(&mut bytes, 256).expect("encode failed");

        assert_eq!(bytes.len(), 2);
        assert_eq!(vli_len_u64(256), 2);

        let mut bytes_ref: &[u8] = bytes.as_ref();
        let decode_int = decode_u64(&mut bytes_ref).expect("decode err");

        assert_eq!(decode_int, 256);
    }

    #[test]
    fn test_encode_3bytes() {
        let num: u64 = 16883;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 3);
        assert_eq!(vli_len_u64(num), 3);

        let mut bytes_ref: &[u8] = bytes.as_ref();
        let decode_int = decode_u64(&mut bytes_ref).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_4bytes() {
        let num: u64 = 2097152;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 4);
        assert_eq!(vli_len_u64(num), 4);

        let mut bytes_ref: &[u8] = bytes.as_ref();
        let decode_int = decode_u64(&mut bytes_ref).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_5bytes() {
        let num: u64 = 34359738000;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 5);
        assert_eq!(vli_len_u64(num), 5);

        let mut bytes_ref: &[u8] = bytes.as_ref();
        let decode_int = decode_u64(&mut bytes_ref).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_6bytes() {
        let num: u64 = 1099511627000;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 6);
        assert_eq!(vli_len_u64(num), 6);

        let mut bytes_ref: &[u8] = bytes.as_ref();
        let decode_int = decode_u64(&mut bytes_ref).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_8bytes() {
        let num: u64 = 0b11100011 << 51;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 8);
        assert_eq!(vli_len_u64(num), 8);

        let mut bytes_ref: &[u8] = bytes.as_ref();
        let decode_int = decode_u64(&mut bytes_ref).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_9bytes() {
        let num: u64 = 0b11100011 << 56;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 9);
        assert_eq!(vli_len_u64(num), 9);

        let mut bytes_ref: &[u8] = bytes.as_ref();
        let decode_int = decode_u64(&mut bytes_ref).expect("decode err");

        assert_eq!(decode_int, num)
    }

}
