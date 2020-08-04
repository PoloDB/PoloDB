use crate::db::DbResult;
use crate::error::DbErr;
use std::io::Write;
use std::slice::Iter;

// http://www.dlugosz.com/ZIP2/VLI.html

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
// 111 11 111	n	    any	        Any multi-precision integer

static BYTE_MARK1: u8 = 0b10000000;
static BYTE_MARK2: u8 = 0b11000000;
static BYTE_MARK3: u8 = 0b11100000;
static BYTE_MARK5: u8 = 0b11111000;

pub fn encode(writer: &mut dyn Write, num: i64) -> DbResult<()> {
    encode_u64(writer, num as u64)
}

#[inline]
fn encode_u64(writer: &mut dyn Write, num: u64) -> DbResult<()> {
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
    } else if num <= 34359738368 {  // 5 bytes
        let num: u64 = 0b11101000 << 32 | num;
        writer.write_all(num.to_be_bytes()[3..8].as_ref())?;
    } else if num <= 0x1FFFFFFFFFF {  // 6 bytes
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

fn decode(iter: &mut Iter<u8>) -> DbResult<i64> {
    let tmp = decode_u64(iter)?;
    Ok(tmp as i64)
}

macro_rules! try_read_byte {
    ($curIter:expr) => {
        match $curIter {
            Some(byte) => *byte,
            None => return Err(DbErr::DecodeEOF),
        }
    }
}

#[inline]
fn decode_u64(iter: &mut Iter<u8>) -> DbResult<u64> {
    let first_byte = try_read_byte!(iter.next());

    if (first_byte & BYTE_MARK1) == 0 {  // 1 byte
        return Ok(first_byte as u64)
    }

    if first_byte & BYTE_MARK2 == 0b10000000 {  // 2 bytes
        let one_more = try_read_byte!(iter.next());

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
        tmp[2] = try_read_byte!(iter.next());
        tmp[3] = try_read_byte!(iter.next());

        return Ok(u32::from_be_bytes(tmp) as u64)
    }

    match first_byte & BYTE_MARK5 {  // three arms
        0b11100000 => {  // 4 bytes
            let mut tmp: [u8; 4] = [0; 4];

            tmp[0] = first_byte & (!BYTE_MARK5);
            tmp[1] = try_read_byte!(iter.next());
            tmp[2] = try_read_byte!(iter.next());
            tmp[3] = try_read_byte!(iter.next());

            return Ok(u32::from_be_bytes(tmp) as u64)
        }

        0b11101000 => {  // 5 bytes
            let mut tmp: [u8; 8] = [0; 8];

            tmp[3] = first_byte & (!BYTE_MARK5);
            for i in 4..8 {
                tmp[i] = try_read_byte!(iter.next());
            }

            return Ok(u64::from_be_bytes(tmp))
        }

        0b11110000 => {  // 8 bytes
            let mut tmp: [u8; 8] = [0; 8];

            tmp[0] = first_byte & (!BYTE_MARK5);
            for i in 1..8 {
                tmp[i] = try_read_byte!(iter.next());
            }

            return Ok(u64::from_be_bytes(tmp))
        }

        _ => ()
    }

    if first_byte == 0b11111000 {  // 6 bytes
        let mut tmp: [u8; 8] = [0; 8];
        for i in 3..8 {
            tmp[i] = try_read_byte!(iter.next());
        }

        return Ok(u64::from_be_bytes(tmp));
    }

    if first_byte == 0b11111001 {  // 9 bytes
        let mut tmp: [u8; 8] = [0; 8];
        for i in 0..8 {
            tmp[i] = try_read_byte!(iter.next());
        }

        return Ok(u64::from_be_bytes(tmp));
    }

    Err(DbErr::DecodeIntUnknownByte)
}

#[cfg(test)]
mod tests {
    use crate::vli::{encode_u64, decode_u64};

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

        let mut iter = bytes.iter();
        let decode_int = decode_u64(&mut iter).expect("decode err");

        assert_eq!(decode_int, 256);
    }

    #[test]
    fn test_encode_3bytes() {
        let num: u64 = 16883;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 3);

        let mut iter = bytes.iter();
        let decode_int = decode_u64(&mut iter).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_4bytes() {
        let num: u64 = 2097152;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 4);

        let mut iter = bytes.iter();
        let decode_int = decode_u64(&mut iter).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_5bytes() {
        let num: u64 = 34359738000;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 5);

        let mut iter = bytes.iter();
        let decode_int = decode_u64(&mut iter).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_6bytes() {
        let num: u64 = 1099511627000;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 6);

        let mut iter = bytes.iter();
        let decode_int = decode_u64(&mut iter).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_8bytes() {
        let num: u64 = 0b11100011 << 51;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 8);

        let mut iter = bytes.iter();
        let decode_int = decode_u64(&mut iter).expect("decode err");

        assert_eq!(decode_int, num)
    }

    #[test]
    fn test_9bytes() {
        let num: u64 = 0b11100011 << 56;

        let mut bytes = vec![];

        encode_u64(&mut bytes, num).expect("encode error");

        assert_eq!(bytes.len(), 9);

        let mut iter = bytes.iter();
        let decode_int = decode_u64(&mut iter).expect("decode err");

        assert_eq!(decode_int, num)
    }

}
