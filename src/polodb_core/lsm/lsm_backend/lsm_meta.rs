use std::num::NonZeroU32;
use crate::page::RawPage;

static HEADER_DESP: &str          = "PoloDB Format v4.0";
const PAGE_SIZE_OFFSET: u32       = 40;
const BLOCK_SIZE_OFFSET: u32      = 44;

/**
 * Offset 0 (32 bytes) : "PoloDB Format v3.0";
 * Offset 32 (8 bytes) : MetaId;
 * Offset 40 (4 bytes) : PageSize;
 * Offset 44 (4 bytes) : BlockSize;
 */
pub(crate) struct LsmMetaDelegate(pub RawPage);

impl LsmMetaDelegate {

    pub fn new(page_size: u32, block_size: u32) -> LsmMetaDelegate {
        let raw_page = RawPage::new(0, NonZeroU32::new(page_size).unwrap());

        let mut delegate = LsmMetaDelegate(raw_page);
        delegate.set_title(HEADER_DESP);
        delegate.set_meta_id(0);
        delegate.set_page_size(page_size);
        delegate.set_block_size(block_size);

        delegate
    }

    #[inline]
    fn set_title(&mut self, title: &str) {
        self.0.seek(0);
        self.0.put_str(title);
    }

    #[inline]
    fn set_meta_id(&mut self, meta_id: u64) {
        self.0.seek(32);
        self.0.put_u64(meta_id);
    }

    #[inline]
    fn set_block_size(&mut self, block_size: u32) {
        self.0.seek(BLOCK_SIZE_OFFSET);
        self.0.put_u32(block_size);
    }

    #[inline]
    fn set_page_size(&mut self, page_size: u32) {
        self.0.seek(PAGE_SIZE_OFFSET);
        self.0.put_u32(page_size);
    }

}
