use std::fs::File;
use std::collections::BTreeMap;
use std::io::{Seek, Write, SeekFrom};
use libc::rand;
use crate::page::RawPage;
use crate::crc64::crc64;

static HEADER_DESP: &str       = "PipeappleDB Journal v0.1";

// 40 bytes
pub struct FrameHeader {
    page_id:       u32,  // offset 0
    db_size:       u64,  // offset 8
    salt1:         u32,  // offset 16
    salt2:         u32,  // offset 20
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
    salt1:            u32,
    salt2:            u32,

    // page_id => file_position
    offset_map:       BTreeMap<u32, u64>,
    count:            u32,
}

fn generate_a_salt() -> u32 {
    unsafe {
        rand() as u32
    }
}

fn journal_check_header(file: &mut File, page_size: u32) -> std::io::Result<(u32, u32)> {
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

    let salt_1 = generate_a_salt();
    let salt_1_be = salt_1.to_be_bytes();
    header48[40..44].copy_from_slice(&salt_1_be);

    let salt_2 = generate_a_salt();
    let salt_2_be = salt_2.to_be_bytes();
    header48[44..48].copy_from_slice(&salt_2_be);

    file.write(&header48)?;

    let checksum = crc64(0, &header48);
    let checksum_be = checksum.to_be_bytes();

    file.seek(SeekFrom::Start(48))?;
    file.write(&checksum_be)?;

    Ok((salt_1, salt_2))
}

fn journal_init_header(file: &mut File) -> std::io::Result<(u32, u32)> {
    file.set_len(64)?;
    Ok((0, 0))
}

impl JournalManager {

    pub fn open(path: &str, page_size: u32) -> std::io::Result<JournalManager> {
        let mut journal_file = File::create(path)?;
        let meta = journal_file.metadata()?;

        let (salt1, salt2) = if meta.len() == 0 {
            journal_check_header(&mut journal_file, page_size)?
        } else {
            journal_init_header(&mut journal_file)?
        };

        Ok(JournalManager {
            journal_file,
            block_size: 4096,
            salt1, salt2,
            offset_map: BTreeMap::new(),
            count: 0,
        })
    }

    // frame_header: 24 bytes
    // checksum1:    8 bytes(offset 24)  header24 checksum
    // checksum2:    8 bytes(offset 32)  page checksum
    // data_begin:   page size(offset 40)
    pub fn append_frame_header(&mut self, frame_header: &FrameHeader, checksum2: u64) -> std::io::Result<()> {
        let mut header24 = vec![];
        header24.resize(24, 0);

        let page_id_be = frame_header.page_id.to_be_bytes();
        header24[0..4].copy_from_slice(&page_id_be);

        let db_size_be = frame_header.db_size.to_be_bytes();
        header24[8..16].copy_from_slice(&db_size_be);

        let salt1_be = frame_header.salt1.to_be_bytes();
        header24[16..20].copy_from_slice(&salt1_be);

        let salt2_be = frame_header.salt2.to_be_bytes();
        header24[20..24].copy_from_slice(&salt2_be);

        self.journal_file.seek(SeekFrom::End(0))?;
        self.journal_file.write(&header24)?;

        let checksum1 = crc64(0, &header24);
        let checksum1_be = checksum1.to_be_bytes();
        self.journal_file.write(&checksum1_be)?;

        let checksum2_be = checksum2.to_be_bytes();
        self.journal_file.write(&checksum2_be)?;

        Ok(())
    }

    pub(crate) fn append_raw_page(&mut self, raw_page: &RawPage) -> std::io::Result<()> {
        let start_pos = self.journal_file.seek(SeekFrom::Current(0))?;

        let frame_header = FrameHeader {
            page_id: raw_page.page_id,
            db_size: 0,
            salt1: self.salt1,
            salt2: self.salt2,
        };

        // calculate checksum of page data
        let checksum2 = crc64(0, &raw_page.data);

        self.append_frame_header(&frame_header, checksum2)?;

        self.journal_file.write(&raw_page.data)?;

        self.offset_map.insert(raw_page.page_id, start_pos);
        self.count += 1;

        Ok(())
    }

    pub(crate) fn read_page(&mut self, page_id: u32) -> std::io::Result<Option<RawPage>> {
        let offset = match self.offset_map.get(&page_id) {
            Some(offset) => *offset,
            None => return Ok(None),
        };
        let data_offset = offset + 40;

        self.journal_file.seek(SeekFrom::Start(data_offset))?;

        let mut result = RawPage::new(page_id, self.block_size);
        result.read_from_file(&mut self.journal_file, data_offset)?;

        Ok(Some(result))
    }

    pub(crate) fn checkpoint_finished(&mut self) -> std::io::Result<()> {
        self.journal_file.set_len(64)?;  // truncate file to 64 bytes

        // clear all data
        self.salt1 += 1;
        self.count = 0;
        self.offset_map.clear();

        // TODO: write header48

        Ok(())
    }

    #[inline]
    pub(crate) fn len(&self) -> u32 {
        self.count
    }

}
