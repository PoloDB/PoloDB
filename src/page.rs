use std::io;
use std::fmt;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write, Read};
use std::sync::Weak;
use std::sync::Arc;

use super::db::DbContext;

enum PageType {
    Undefined = 0,

    FileHeader,

    Collection,

    BTreeNode,

}

#[derive(Debug)]
pub struct RawPage {
    data:          Vec<u8>,
    pos:           usize,
}

#[derive(Debug, Clone)]
pub struct SpaceNotEnough;

impl fmt::Display for SpaceNotEnough {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "page space is not enough")
    }
}

impl RawPage {

    pub fn new(size: usize) -> RawPage {
        let mut v: Vec<u8> = Vec::new();
        v.resize(size, 0);
        RawPage {
            data: v,
            pos: 0,
        }
    }

    pub fn put(&mut self, data: &[u8]) -> Result<(), SpaceNotEnough> {
        if data.len() + self.pos > self.data.len() {
            return Err(SpaceNotEnough);
        }

        unsafe {
            self.data.as_mut_ptr().offset(self.pos as isize).copy_from(data.as_ptr(), data.len());
        }
        self.pos += data.len();

        Ok(())
    }

    pub fn put_str(&mut self, str: &str) -> Result<(), SpaceNotEnough> {
        if str.len() + self.pos > self.data.len() {
            return Err(SpaceNotEnough);
        }

        unsafe {
            self.data.as_mut_ptr().offset(self.pos as isize).copy_from(str.as_ptr(), str.len());
        }
        self.pos += str.len();

        Ok(())
    }

    pub fn get_u32(&self, pos: usize) -> u32 {
        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&self.data[pos..(pos + 4)]);
        u32::from_be_bytes(buffer)
    }

    pub fn get_u64(&self, pos: usize) -> u64 {
        let mut buffer: [u8; 8] = [0; 8];
        buffer.copy_from_slice(&self.data[pos..(pos + 8)]);
        u64::from_be_bytes(buffer)
    }

    pub fn sync_to_file(&self, file: &mut File, offset: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(offset))?;
        file.write(self.data.as_slice())?;
        Ok(())
    }

    pub fn read_from_file(&mut self, file: &mut File, offset: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(offset))?;
        file.read(self.data.as_mut_slice())?;
        Ok(())
    }

    pub fn seek(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub fn len(&self) {
        self.data.len();
    }

}

struct FreeList {
    free_list_page_id:   u32,
    data:                Vec<u32>,
}

static FREE_LIST_OFFSET: usize = 2048;

impl FreeList {

    fn new() -> FreeList {
        FreeList {
            free_list_page_id: 0,
            data: Vec::new(),
        }
    }

    fn from_raw(raw_page: &RawPage) -> FreeList {
        let size = raw_page.get_u32(FREE_LIST_OFFSET);
        let free_list_page_id = raw_page.get_u32(FREE_LIST_OFFSET + 4);

        let mut data: Vec<u32> = Vec::new();
        data.resize(size as usize, 0);

        for i in 0..size {
            let offset = FREE_LIST_OFFSET + 8 + (i * 4) as usize;
            data.insert(i as usize, raw_page.get_u32(offset));
        }

        FreeList {
            free_list_page_id,
            data,
        }
    }
    
}

#[derive(Debug)]
pub struct HeaderPage {
    title:      String,
    version:      [u8; 4],
    sector_size:  u32,
    page_size:    u32
}

fn parse_version(raw_page: &RawPage, version: &mut [u8; 4]) {
    for i in 0..4 {
        version[i] = raw_page.data[32 + i];
    }
}

impl HeaderPage {

    pub fn new() -> HeaderPage {
        HeaderPage {
            title: "PipeappleDB Format v0.1".to_string(),
            version: [0, 0, 0, 0],
            sector_size: 4096,
            page_size: 4096,
        }
    }

    pub fn from_raw(raw_page: &RawPage) -> Option<HeaderPage> {
        let mut zero_pos: i32 = -1;
        for i in 0..32 {
            if raw_page.data[i] == 0 {
                zero_pos = i as i32;
                break;
            }
        }

        if zero_pos < 0 {
            return None;
        }

        let title = String::from_utf8_lossy(&raw_page.data[0..(zero_pos as usize)]);

        let mut version: [u8; 4] = [0; 4];
        parse_version(&raw_page, &mut version);

        let sector_size = raw_page.get_u32(40);
        let page_size = raw_page.get_u32(44);

        Some(HeaderPage {
            title: title.to_string(),
            version,
            sector_size,
            page_size,
        })
    }

    pub fn to_raw(&self) -> RawPage {
        let mut result = RawPage::new(self.page_size as usize);

        result.put_str(self.title.as_str());

        result.seek(32);
        result.put(&self.version);

        let sector_be = self.sector_size.to_be_bytes();

        result.seek(40);
        result.put(&sector_be);

        let page_size_be = self.page_size.to_be_bytes();
        result.seek(44);
        result.put(&page_size_be);

        result
    }

}

struct PageManager {
    ctx: Weak<DbContext>,
}

impl PageManager {

    fn new(ctx: &Arc<DbContext>) -> PageManager {
        let weak = Arc::downgrade(ctx);
        PageManager {
            ctx: weak,
        }
    }

}

#[cfg(test)]
mod tests {
    use crate::page::HeaderPage;

    #[test]
    fn parse_and_gen() {
        let header = HeaderPage::new();
        let raw_page = header.to_raw();
        let header2 = HeaderPage::from_raw(&raw_page).expect("should has header");

        assert_eq!(header.title, header2.title);
        assert_eq!(header.page_size, header2.page_size);
        assert_eq!(header.version, header2.version);
        assert_eq!(header.sector_size, header2.sector_size);
    }

}
