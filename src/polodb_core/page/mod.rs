/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) mod header_page_wrapper;
pub(crate) mod data_page_wrapper;
pub(crate) mod free_list_data_wrapper;
pub(crate) mod large_data_page_wrapper;
pub(crate) mod data_allocator_wrapper;

pub(crate) use free_list_data_wrapper::FreeListDataWrapper;

#[cfg(not(target_arch = "wasm32"))]
use std::io::{Seek, SeekFrom, Write, Read};
#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
use std::num::NonZeroU32;

#[repr(u8)]
pub(crate) enum PageType {
    #[allow(dead_code)]
    Undefined = 0,

    BTreeNode,

    #[allow(dead_code)]
    OverflowData,

    Data,

    FreeList,

    LargeData,

    DataAllocator,

}

impl PageType {

    pub fn to_magic(self) -> [u8; 2] {
        [0xFF, self as u8]
    }

}

#[derive(Debug, Clone)]
pub(crate) struct RawPage {
    pub page_id:    u32,
    pub data:       Vec<u8>,
    pos:            u32,
}

impl RawPage {

    pub fn new(page_id: u32, size: NonZeroU32) -> RawPage {
        let mut v: Vec<u8> = Vec::new();
        v.resize(size.get() as usize, 0);
        RawPage {
            page_id,
            data: v,
            pos: 0,
        }
    }

    #[allow(dead_code)]
    pub unsafe fn copy_from_ptr(&mut self, ptr: *const u8) {
        let target_ptr = self.data.as_mut_ptr();
        target_ptr.copy_from_nonoverlapping(ptr, self.data.len());
    }

    #[allow(dead_code)]
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
        self.data[self.pos as usize] = data;
        self.pos += 1;
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
    #[allow(dead_code)]
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

    #[cfg(not(target_arch = "wasm32"))]
    pub fn sync_to_file(&self, file: &mut File, offset: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(self.data.as_slice())?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
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
    pub fn len(&self) -> u32 {
        self.data.len() as u32
    }

}
