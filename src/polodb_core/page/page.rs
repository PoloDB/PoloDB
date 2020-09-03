use std::fs::File;
use std::io::{Seek, SeekFrom, Write, Read};
use super::header_page_wrapper;
use crate::DbResult;
use crate::error::{DbErr, parse_error_reason};

#[repr(u8)]
#[allow(dead_code)]
pub(crate) enum PageType {
    Undefined = 0,

    BTreeNode,

    OverflowData,

}

impl PageType {

    pub fn to_magic(self) -> [u8; 2] {
        [0xFF, self as u8]
    }

    pub fn from_magic(magic: [u8; 2]) -> DbResult<PageType> {
        if magic[0] != 0xFF {
            return Err(DbErr::ParseError(parse_error_reason::UNEXPECTED_PAGE_HEADER.into()));
        }

        match magic[1] {
            0 => Ok(PageType::Undefined),

            1 => Ok(PageType::BTreeNode),

            2 => Ok(PageType::OverflowData),

            _ => Err(DbErr::ParseError(parse_error_reason::UNEXPECTED_PAGE_TYPE.into()))
        }
    }

}

#[derive(Debug)]
pub(crate) struct RawPage {
    pub page_id:    u32,
    pub data:       Vec<u8>,
    pos:            u32,
}

impl RawPage {

    pub fn new(page_id: u32, size: u32) -> RawPage {
        let mut v: Vec<u8> = Vec::new();
        v.resize(size as usize, 0);
        RawPage {
            page_id,
            data: v,
            pos: 0,
        }
    }

    pub unsafe fn copy_from_ptr(&mut self, ptr: *const u8) {
        let target_ptr = self.data.as_mut_ptr();
        target_ptr.copy_from_nonoverlapping(ptr, self.data.len());
    }

    pub unsafe fn copy_to_ptr(&self, ptr: *mut u8) {
        let target_ptr = self.data.as_ptr();
        target_ptr.copy_to_nonoverlapping(ptr, self.data.len());
    }

    pub fn put(&mut self, data: &[u8]) {
        if data.len() + self.pos as usize > self.data.len() {
            panic!("space is not enough for page");
        }

        unsafe {
            self.data.as_mut_ptr().offset(self.pos as isize)
                .copy_from_nonoverlapping(data.as_ptr(), data.len());
        }

        self.pos += data.len() as u32;
    }

    pub fn put_str(&mut self, str: &str) {
        if str.len() + self.pos as usize > self.data.len() {
            panic!("space is not enough for page");
        }

        unsafe {
            self.data.as_mut_ptr().offset(self.pos as isize).copy_from_nonoverlapping(str.as_ptr(), str.len());
        }

        self.pos += str.len() as u32;
    }

    #[allow(dead_code)]
    pub fn get_u8(&self, pos: u32) -> u8 {
        self.data[pos as usize]
    }

    #[inline]
    #[allow(dead_code)]
    pub fn put_u8(&mut self, data: u8) {
        self.data[self.pos as usize] = data
    }

    #[inline]
    pub fn get_u16(&self, pos: u32) -> u16 {
        let mut buffer: [u8; 2] = [0; 2];
        buffer.copy_from_slice(&self.data[(pos as usize)..((pos as usize) + 2)]);
        u16::from_be_bytes(buffer)
    }

    #[inline]
    pub fn put_u16(&mut self, data: u16) {
        let data_be = data.to_be_bytes();
        self.put(&data_be)
    }

    #[inline]
    pub fn get_u32(&self, pos: u32) -> u32 {
        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&self.data[(pos as usize)..((pos as usize) + 4)]);
        u32::from_be_bytes(buffer)
    }

    #[inline]
    pub fn put_u32(&mut self, data: u32) {
        let data_be = data.to_be_bytes();
        self.put(&data_be)
    }

    #[inline]
    pub fn put_u64(&mut self, data: u64) {
        let data_be = data.to_be_bytes();
        self.put(&data_be)
    }

    #[inline]
    #[allow(dead_code)]
    pub fn get_u64(&self, pos: u32) -> u64 {
        let mut buffer: [u8; 8] = [0; 8];
        buffer.copy_from_slice(&self.data[(pos as usize)..((pos as usize) + 8)]);
        u64::from_be_bytes(buffer)
    }

    pub fn sync_to_file(&self, file: &mut File, offset: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(offset))?;
        file.write(self.data.as_slice())?;
        Ok(())
    }

    pub fn read_from_file(&mut self, file: &mut File, offset: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(self.data.as_mut_slice())?;
        Ok(())
    }

    #[inline]
    pub fn seek(&mut self, pos: u32) {
        self.pos = pos;
    }

    #[inline]
    #[allow(dead_code)]
    pub fn len(&self) -> u32 {
        self.data.len() as u32
    }

}

struct FreeList {
    free_list_page_id:   u32,
    data:                Vec<u32>,
}

impl FreeList {

    fn new() -> FreeList {
        FreeList {
            free_list_page_id: 0,
            data: Vec::new(),
        }
    }

    fn from_raw(raw_page: &RawPage) -> FreeList {
        let size = raw_page.get_u32(header_page_wrapper::FREE_LIST_OFFSET);
        let free_list_page_id = raw_page.get_u32(header_page_wrapper::FREE_LIST_OFFSET + 4);

        let mut data: Vec<u32> = Vec::new();
        data.resize(size as usize, 0);

        for i in 0..size {
            let offset = header_page_wrapper::FREE_LIST_OFFSET + 8 + (i * 4);
            data.insert(i as usize, raw_page.get_u32(offset));
        }

        FreeList {
            free_list_page_id,
            data,
        }
    }
    
}
