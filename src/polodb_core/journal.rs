use std::fs::File;
use std::path::{Path, PathBuf};
use std::collections::{LinkedList, BTreeMap};
use std::io::{Seek, Write, SeekFrom, Read};
use libc::rand;
use crate::page::RawPage;
use crate::crc64::crc64;
use crate::DbResult;
use crate::error::DbErr;

static HEADER_DESP: &str       = "PipeappleDB Journal v0.1";
const JOURNAL_DATA_BEGIN: u32 = 64;
const FRAME_HEADER_SIZE: u32  = 40;

// 24 bytes
pub(crate) struct FrameHeader {
    page_id:       u32,  // offset 0
    db_size:       u64,  // offset 8
    salt1:         u32,  // offset 16
    salt2:         u32,  // offset 20
}

impl FrameHeader {

    fn from_bytes(bytes: &[u8]) -> FrameHeader {
        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&bytes[0..4]);

        let page_id = u32::from_be_bytes(buffer);

        let mut buffer: [u8; 8] = [0; 8];
        buffer.copy_from_slice(&bytes[8..16]);
        let db_size = u64::from_be_bytes(buffer);

        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&bytes[16..20]);
        let salt1 = u32::from_be_bytes(buffer);

        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&bytes[20..24]);
        let salt2 = u32::from_be_bytes(buffer);

        FrameHeader {
            page_id,
            db_size,
            salt1, salt2
        }
    }

}

pub enum TransactionType {
    Read,
    Write,
}

// name:       32 bytes
// version:    4bytes(offset 32)
// page_size:  4bytes(offset 36)
// salt_1:     4bytes(offset 40)
// salt_2:     4bytes(offset 44)
// checksum before 48:   8bytes(offset 48)
// data begin: 64 bytes
pub(crate) struct JournalManager {
    file_path:        PathBuf,
    journal_file:     File,
    version:          [u8; 4],
    page_size:        u32,
    salt1:            u32,
    salt2:            u32,
    transaction_ty:   Option<TransactionType>,

    // page_id => file_position
    pub offset_map_list:       LinkedList<BTreeMap<u32, u64>>,
    count:            u32,
}

fn generate_a_salt() -> u32 {
    unsafe {
        rand() as u32
    }
}

impl JournalManager {

    pub fn open(path: &Path, page_size: u32) -> DbResult<JournalManager> {
        let journal_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let meta = journal_file.metadata()?;

        let mut offset_map_list: LinkedList<BTreeMap<u32, u64>> = LinkedList::new();
        offset_map_list.push_back(BTreeMap::new());

        let file_path: PathBuf = path.to_path_buf();
        let mut result = JournalManager {
            file_path,
            journal_file,
            version: [0, 0, 1, 0],
            page_size,
            salt1: 0,
            salt2: 0,
            transaction_ty: None,

            offset_map_list,
            count: 0,
        };

        if meta.len() == 0 {  // init the file
            result.init_header_to_file()?;
        } else {
            result.read_and_check_from_file()?;
        }

        result.journal_file.seek(SeekFrom::Start(JOURNAL_DATA_BEGIN as u64))?;
        result.load_all_pages(meta.len())?;

        Ok(result)
    }

    fn init_header_to_file(&mut self) -> DbResult<()> {
        self.salt1 = generate_a_salt();
        self.salt2 = generate_a_salt();
        self.write_header_to_file()
    }

    fn write_header_to_file(&mut self) -> DbResult<()> {
        let mut header48: Vec<u8> = vec![];
        header48.resize(48, 0);

        // copy title
        let title_bytes = HEADER_DESP.as_bytes();
        header48[0..title_bytes.len()].copy_from_slice(title_bytes);

        // copy version
        header48[32..36].copy_from_slice(&self.version);

        // write page_size
        let page_size_be = self.page_size.to_be_bytes();
        header48[36..40].copy_from_slice(&page_size_be);

        let salt_1_be = self.salt1.to_be_bytes();
        header48[40..44].copy_from_slice(&salt_1_be);

        let salt_2_be = self.salt2.to_be_bytes();
        header48[44..48].copy_from_slice(&salt_2_be);

        self.journal_file.seek(SeekFrom::Start(0))?;
        self.journal_file.write(&header48)?;

        let checksum = crc64(0, &header48);
        let checksum_be = checksum.to_be_bytes();

        self.journal_file.seek(SeekFrom::Start(48))?;
        self.journal_file.write(&checksum_be)?;

        Ok(())
    }

    fn read_and_check_from_file(&mut self) -> DbResult<()> {
        let mut header48: Vec<u8> = Vec::with_capacity(48);
        header48.resize(48, 0);
        self.journal_file.read_exact(&mut header48)?;

        let checksum = crc64(0, &header48);
        let checksum_from_file = self.read_checksum_from_file()?;
        if checksum != checksum_from_file {
            return Err(DbErr::ChecksumMismatch);
        }

        // copy version
        self.version.copy_from_slice(&header48[32..36]);

        self.page_size = {
            let mut buffer: [u8; 4] = [0; 4];
            buffer.copy_from_slice(&header48[36..40]);
            let actual_page_size = u32::from_be_bytes(buffer);

            if actual_page_size != self.page_size {
                return Err(DbErr::JournalPageSizeMismatch(actual_page_size, self.page_size));
            }

            actual_page_size
        };

        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&header48[40..44]);
        self.salt1 = u32::from_be_bytes(buffer);

        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&header48[44..48]);
        self.salt2 = u32::from_be_bytes(buffer);

        Ok(())
    }

    fn read_checksum_from_file(&mut self) -> DbResult<u64> {
        self.journal_file.seek(SeekFrom::Start(48))?;
        let mut buffer: [u8; 8] = [0; 8];
        self.journal_file.read_exact(&mut buffer)?;
        Ok(u64::from_be_bytes(buffer))
    }

    fn load_all_pages(&mut self, file_size: u64) -> DbResult<()> {
        let mut current_pos = self.journal_file.seek(SeekFrom::Current(0))?;
        let frame_size = (self.page_size as u64) + (FRAME_HEADER_SIZE as u64);

        while current_pos + frame_size <= file_size {
            let mut buffer = vec![];
            buffer.resize(frame_size as usize, 0);

            self.journal_file.read_exact(&mut buffer)?;

            match self.check_and_load_frame(current_pos, &buffer) {
                Ok(()) => (),
                Err(DbErr::SaltMismatch) |
                Err(DbErr::ChecksumMismatch) => {
                    self.journal_file.set_len(current_pos)?;  // trim the tail
                    self.journal_file.seek(SeekFrom::End(0))?;  // recover position
                    break;  // finish the loop
                }
                Err(err) => return Err(err),
            }

            self.count += 1;
            current_pos = self.journal_file.seek(SeekFrom::Current(0))?;
        }

        Ok(())
    }

    fn check_and_load_frame(&mut self, current_pos: u64, bytes: &[u8]) -> DbResult<()> {
        let frame_header = FrameHeader::from_bytes(&bytes[0..24]);
        let checksum1 = {
            let mut buffer: [u8; 8] = [0; 8];
            buffer.copy_from_slice(&bytes[24..32]);
            u64::from_be_bytes(buffer)
        };

        let checksum2 = {
            let mut buffer: [u8; 8] = [0; 8];
            buffer.copy_from_slice(&bytes[32..40]);
            u64::from_be_bytes(buffer)
        };

        let actual_header_checksum = crc64(0, &bytes[0..24]);

        if actual_header_checksum != checksum1 {
            return Err(DbErr::ChecksumMismatch);
        }

        let actual_page_checksum = crc64(0, &bytes[(FRAME_HEADER_SIZE as usize)..]);

        if actual_page_checksum != checksum2 {
            return Err(DbErr::ChecksumMismatch);
        }

        if frame_header.salt1 != self.salt1 || frame_header.salt2 != self.salt2 {
            return Err(DbErr::SaltMismatch);
        }

        self.offset_map_list.back_mut().unwrap().insert(frame_header.page_id, current_pos);
        Ok(())
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

        self.journal_file.write(&header24)?;

        let checksum1 = crc64(0, &header24);
        let checksum1_be = checksum1.to_be_bytes();
        self.journal_file.write(&checksum1_be)?;

        let checksum2_be = checksum2.to_be_bytes();
        self.journal_file.write(&checksum2_be)?;

        Ok(())
    }

    pub(crate) fn append_raw_page(&mut self, raw_page: &RawPage) -> DbResult<()> {
        match self.transaction_ty {
            Some(TransactionType::Write) => (),
            _ => return Err(DbErr::CannotWriteDbWithoutTransaction),
        }

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

        self.offset_map_list.back_mut().unwrap().insert(raw_page.page_id, start_pos);
        self.count += 1;

        #[cfg(feature = "log")]
            eprintln!("append page to journal, page_id: {}, start_pos:\t\t0x{:0>8X}", raw_page.page_id, start_pos);

        Ok(())
    }

    pub(crate) fn read_page(&mut self, page_id: u32) -> std::io::Result<Option<RawPage>> {
        let offset = match self.offset_map_list.back().unwrap().get(&page_id) {
            Some(offset) => *offset,
            None => return Ok(None),
        };
        let data_offset = offset + (FRAME_HEADER_SIZE as u64);

        self.journal_file.seek(SeekFrom::Start(data_offset))?;

        let mut result = RawPage::new(page_id, self.page_size);
        result.read_from_file(&mut self.journal_file, data_offset)?;

        #[cfg(feature = "log")]
            eprintln!("read page from journal, page_id: {}, data_offset:\t\t0x{:0>8X}", page_id, offset);

        Ok(Some(result))
    }

    pub(crate) fn checkpoint_journal(&mut self, db_file: &mut File) -> DbResult<()> {
        for (page_id, offset) in self.offset_map_list.back().unwrap() {
            let data_offset = offset + (FRAME_HEADER_SIZE as u64);

            self.journal_file.seek(SeekFrom::Start(data_offset))?;

            let mut result = RawPage::new(*page_id, self.page_size);
            result.read_from_file(&mut self.journal_file, data_offset)?;

            result.sync_to_file(db_file, (*page_id as u64) * (self.page_size as u64))?;
        }

        db_file.flush()?;  // only checkpoint flush the file

        self.checkpoint_finished()
    }

    fn plus_salt1(&mut self) {
        if self.salt1 == u32::max_value() {
            self.salt1 = 0;
            return;
        }
        self.salt1 += 1;
    }

    fn checkpoint_finished(&mut self) -> DbResult<()> {
        self.journal_file.set_len(64)?;  // truncate file to 64 bytes

        // clear all data
        self.count = 0;

        self.offset_map_list.clear();
        self.offset_map_list.push_back(BTreeMap::new());

        self.plus_salt1();
        self.salt2 = generate_a_salt();
        self.write_header_to_file()
    }

    pub(crate) fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()> {
        if self.transaction_ty.is_some() {
            return Err(DbErr::StartTransactionInAnotherTransaction);
        }

        match ty {
            TransactionType::Read => {
                self.shared_lock_file()?;
            }

            TransactionType::Write => {
                self.exclusive_lock_file()?;
            }

        }

        self.transaction_ty = Some(ty);

        Ok(())
    }

    pub(crate) fn commit(&mut self) -> DbResult<()> {
        if self.transaction_ty.is_none() {
            return Err(DbErr::CannotWriteDbWithoutTransaction);
        }

        self.unlock_file()?;
        self.transaction_ty = None;

        Ok(())
    }

    pub(crate) fn rollback(&mut self) -> DbResult<()> {
        if self.transaction_ty.is_none() {
            return Err(DbErr::RollbackNotInTransaction);
        }

        self.unlock_file()?;
        self.transaction_ty = None;

        Ok(())
    }

    pub(crate) fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()> {
        self.exclusive_lock_file()
    }

    fn exclusive_lock_file(&mut self) -> DbResult<()> {
        use std::os::unix::prelude::*;
        use libc::{flock, LOCK_EX, LOCK_NB};

        let fd = self.journal_file.as_raw_fd();
        let result = unsafe {
            flock(fd, LOCK_EX | LOCK_NB)
        };

        if result == 0 {
            Ok(())
        } else {
            Err(DbErr::Busy)
        }
    }

    fn shared_lock_file(&mut self) -> DbResult<()> {
        use std::os::unix::prelude::*;
        use libc::{flock, LOCK_SH, LOCK_NB};

        let fd = self.journal_file.as_raw_fd();
        let result = unsafe {
            flock(fd, LOCK_SH | LOCK_NB)
        };

        if result == 0 {
            Ok(())
        } else {
            Err(DbErr::Busy)
        }
    }

    /// LOCK_UN: unlock
    /// LOCK_NB: non-blocking
    fn unlock_file(&mut self) -> DbResult<()> {
        use std::os::unix::prelude::*;
        use libc::{flock, LOCK_UN, LOCK_NB};

        let fd = self.journal_file.as_raw_fd();
        let result = unsafe {
            flock(fd, LOCK_UN | LOCK_NB)
        };

        if result == 0 {
            Ok(())
        } else {
            Err(DbErr::Busy)
        }
    }

    #[inline]
    pub(crate) fn path(&self) -> &Path {
        self.file_path.as_path()
    }

    #[inline]
    pub(crate) fn len(&self) -> u32 {
        self.count
    }

    #[inline]
    pub(crate) fn transaction_type(&self) -> &Option<TransactionType> {
        &self.transaction_ty
    }

}

#[cfg(test)]
mod tests {
    use crate::journal::JournalManager;
    use crate::page::RawPage;

    static TEST_PAGE_LEN: u32 = 100;

    fn make_raw_page(page_id: u32) -> RawPage {
        let mut page = RawPage::new(page_id, 4096);

        for i in 0..4096 {
            page.data[i] = unsafe {
                libc::rand() as u8
            }
        }

        page
    }

    #[test]
    fn test_journal() {
        let _ = std::fs::remove_file("/tmp/test-journal");
        let mut journal_manager = JournalManager::open("/tmp/test-journal".as_ref(), 4096).unwrap();

        let mut ten_pages = Vec::with_capacity(TEST_PAGE_LEN as usize);

        for i in 0..TEST_PAGE_LEN {
            ten_pages.push(make_raw_page(i))
        }

        for item in &ten_pages {
            journal_manager.append_raw_page(item).unwrap();
        }

        for i in 0..TEST_PAGE_LEN {
            let page = journal_manager.read_page(i).unwrap().unwrap();

            for (index, ch) in page.data.iter().enumerate() {
                assert_eq!(*ch, ten_pages[i as usize].data[index])
            }
        }
    }

}
