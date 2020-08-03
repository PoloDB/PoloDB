use std::fs::File;
use std::io;
use crate::page::RawPage;
use std::io::{Seek, Write, SeekFrom};
use libc::rand;
use crate::crc64::crc64;

static HEADER_DESP: &str       = "PipeappleDB Journal v0.1";

enum JournalType  {
    Invalid = 0,

    NewPage,

    WritePage,

    DeletePage,

}

struct Journal {
    ty: JournalType,
    __reserved0: u16,
    current_jid: i32,
    origin_jid: i64,
}

// name:       32 bytes
// version:    4bytes(offset 32)
// page_size:  4bytes(offset 36)
// salt_1:     4bytes(offset 40)
// salt_2:     4bytes(offset 44)
// checksum before 48:   8bytes(offset 48)
// data begin: 64 bytes
pub struct JournalManager {
    journal_file:     File,
    block_size:       u32,
}

fn generate_a_salt() -> u32 {
    unsafe {
        rand() as u32
    }
}

fn journal_check_header(file: &mut File, page_size: u32) -> std::io::Result<()> {
    let mut header48: Vec<u8> = vec![];
    header48.resize(48, 0);

    // copy title
    let title_bytes = HEADER_DESP.as_bytes();
    header48[0..title_bytes.len()].copy_from_slice(title_bytes);

    // copy version
    let version = [0, 0, 0, 1];
    header48[32..36].copy_from_slice(&version);

    // write page_size
    let page_size_be = page_size.to_be_bytes();
    header48[36..40].copy_from_slice(&page_size_be);

    let salt_1_be = generate_a_salt().to_be_bytes();
    header48[40..44].copy_from_slice(&salt_1_be);

    let salt_2 = generate_a_salt();
    let salt_2_be = salt_2.to_be_bytes();
    header48[44..48].copy_from_slice(&salt_2_be);

    file.write(&header48);

    let checksum = crc64(0, &header48);
    let checksum_be = checksum.to_be_bytes();

    file.seek(SeekFrom::Start(48))?;
    file.write(&checksum_be)?;

    file.flush()?;

    Ok(())
}

fn journal_init_header(file: &mut File) -> std::io::Result<()> {
    file.set_len(64)?;
    Ok(())
}

impl JournalManager {

    pub fn open(path: &str, page_size: u32) -> std::io::Result<JournalManager> {
        let mut journal_file = File::create(path)?;
        let meta = journal_file.metadata()?;

        if meta.len() == 0 {
            journal_check_header(&mut journal_file, page_size);
        } else {
            journal_init_header(&mut journal_file);
        }

        Ok(JournalManager {
            journal_file,
            block_size: 4096,
        })
    }

}
