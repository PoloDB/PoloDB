/**
 * Offset 0 (32 bytes) : "PipeappleDB Format v0.1";
 * Offset 32 (8 bytes) : Version 0.0.0.0;
 * Offset 40 (4 bytes) : SectorSize;
 * Offset 44 (4 bytes) : PageSize;
 * Offset 48 (4 bytes) : NullPageBarId;
 * Offset 52 (4 bytes) : MetaPageId(usually 1);
 *
 * Free list offset: 2048;
 * | 4b   | 4b                  | 4b     | 4b    | ... |
 * | size | free list page link | free 1 | free2 | ... |
 */
use super::page::RawPage;

static HEADER_DESP: &str         = "PipeappleDB Format v0.1";
static SECTOR_SIZE_OFFSET: u32   = 40;
static PAGE_SIZE_OFFSET: u32     = 44;
static NULL_PAGE_BAR_OFFSET: u32 = 48;
static META_PAGE_ID: u32         = 52;
pub static FREE_LIST_OFFSET: u32 = 2048;
static FREE_LIST_PAGE_LINK_OFFSET: u32 = 2048 + 4;
pub static HEADER_FREE_LIST_MAX_SIZE: usize = (2048 - 8) / 4;

pub(crate) fn init(page: &mut RawPage) {
    set_title(page, HEADER_DESP);
    set_version(page, &[0, 0, 0, 1]);
    set_sector_size(page, 4096);
    set_page_size(page, 4096);
    set_meta_page_id(page, 1);
    set_null_page_bar(page, 2);
}

pub(crate) fn set_title(page: &mut RawPage, title: &str) {
    page.seek(0);
    let _ = page.put_str(title);
}

pub(crate) fn get_title(page: &RawPage) -> String {
    let mut zero_pos: i32 = -1;
    for i in 0..32 {
        if page.data[i] == 0 {
            zero_pos = i as i32;
            break;
        }
    }

    if zero_pos < 0 {
        panic!("can not find a zero")
    }

    let title = String::from_utf8_lossy(&page.data[0..(zero_pos as usize)]);
    title.to_string()
}

pub(crate) fn set_version(page: &mut RawPage, version: &[u8]) {
    page.seek(32);
    let _ = page.put(version);
}

pub(crate) fn get_version(page: &RawPage) -> [u8; 4] {
    let mut version: [u8; 4] = [0; 4];
    for i in 0..4 {
        version[i] = page.data[32 + i];
    }
    version
}

#[inline]
pub(crate) fn set_sector_size(page: &mut RawPage, sector_size: u32) {
    page.seek(SECTOR_SIZE_OFFSET);
    let _ = page.put_u32(sector_size);
}

#[inline]
pub(crate) fn get_sector_size(page: &RawPage) -> u32 {
    page.get_u32(SECTOR_SIZE_OFFSET)
}

#[inline]
pub(crate) fn set_page_size(page: &mut RawPage, page_size: u32) {
    page.seek(PAGE_SIZE_OFFSET);
    let _ = page.put_u32(page_size);
}

#[inline]
pub(crate) fn get_page_size(page: &RawPage) -> u32 {
    page.get_u32(PAGE_SIZE_OFFSET)
}

#[inline]
pub(crate) fn get_null_page_bar(page: &RawPage) -> u32 {
    page.get_u32(NULL_PAGE_BAR_OFFSET)
}

#[inline]
pub(crate) fn set_null_page_bar(page: &mut RawPage, data: u32) {
    page.seek(NULL_PAGE_BAR_OFFSET);
    page.put_u32(data)
}

#[inline]
pub(crate) fn get_meta_page_id(page: &RawPage) -> u32 {
    page.get_u32(META_PAGE_ID)
}

#[inline]
pub(crate) fn set_meta_page_id(page: &mut RawPage, data: u32) {
    page.seek(META_PAGE_ID);
    page.put_u32(data)
}

#[inline]
pub(crate) fn get_free_list_size(page: &RawPage) -> u32 {
    page.get_u32(FREE_LIST_OFFSET)
}

#[inline]
pub(crate) fn set_free_list_size(page: &mut RawPage, size: u32) {
    page.seek(FREE_LIST_OFFSET);
    page.put_u32(size)
}

#[inline]
pub(crate) fn get_free_list_content(page: &RawPage, index: u32) -> u32 {
    let offset = index * 4 + FREE_LIST_OFFSET + 8;
    page.get_u32(offset)
}

#[inline]
pub(crate) fn set_free_list_content(page: &mut RawPage, index: u32, pid: u32) {
    let offset = index * 4 + FREE_LIST_OFFSET + 8;
    page.seek(offset);
    page.put_u32(pid);
}

#[inline]
pub(crate) fn set_free_list_page_id(page: &mut RawPage, pid: u32) {
    page.seek(FREE_LIST_PAGE_LINK_OFFSET);
    page.put_u32(pid);
}

#[inline]
pub(crate) fn get_free_list_page_id(page: &RawPage) -> u32 {
    page.get_u32(FREE_LIST_PAGE_LINK_OFFSET)
}

#[cfg(test)]
mod tests {
    // use crate::page::HeaderPage;

    use crate::page::RawPage;
    use crate::page::header_page_utils::*;

    #[test]
    fn parse_and_gen() {
        let mut raw_page = RawPage::new(0, 4096);

        let title = "test title";
        set_title(&mut raw_page, title);
        assert_eq!(get_title(&raw_page), title);

        let test_sector_size = 111;
        set_sector_size(&mut raw_page, test_sector_size);
        assert_eq!(get_sector_size(&raw_page), test_sector_size);

        let test_page_size = 222;
        set_page_size(&mut raw_page, test_page_size);
        assert_eq!(get_page_size(&raw_page), test_page_size);
    }

}
