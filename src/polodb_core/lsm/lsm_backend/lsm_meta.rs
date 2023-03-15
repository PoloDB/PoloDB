/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::num::NonZeroU32;
use crate::lsm::lsm_segment::ImLsmSegment;
use crate::lsm::lsm_snapshot::LsmLevel;
use crate::page::RawPage;

static HEADER_DESP: &str      = "PoloDB Format v4.0";
const META_ID_OFFSET: u32     = 32;
const PAGE_SIZE_OFFSET: u32   = 40;
const BLOCK_SIZE_OFFSET: u32  = 44;
const LOG_OFFSET_OFFSET: u32  = 48;
const LEVEL_COUNT_OFFSET: u32 = 56;
const LEVEL_BEGIN_OFFSET: u32 = 128;

/// Offset 0 (32 bytes) : "PoloDB Format v4.0"
/// Offset 32 (8 bytes) : MetaId
/// Offset 40 (4 bytes) : PageSize
/// Offset 44 (4 bytes) : BlockSize
/// Offset 48 (8 bytes) : LogOffset
/// Offset 56 (1 byte)  : LevelCount
/// Offset 128: Level begin bar
///
/// Level data:
/// 2 bytes: level age
/// 1 byte:  level len
/// 1 byte:  preserve
/// n bytes: records
///
/// Segment records
/// 8 bytes: start pid
/// 8 bytes: end pid
/// 8 bytes: len
///
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
    fn set_block_size(&mut self, block_size: u32) {
        self.0.seek(BLOCK_SIZE_OFFSET);
        self.0.put_u32(block_size);
    }

    #[inline]
    fn set_page_size(&mut self, page_size: u32) {
        self.0.seek(PAGE_SIZE_OFFSET);
        self.0.put_u32(page_size);
    }

    #[inline]
    pub fn set_meta_id(&mut self, meta_id: u64) {
        self.0.seek(META_ID_OFFSET);
        self.0.put_u64(meta_id);
    }

    #[inline]
    pub fn set_log_offset(&mut self, log_offset: u64) {
        self.0.seek(LOG_OFFSET_OFFSET);
        self.0.put_u64(log_offset);
    }

    #[inline]
    pub fn set_level_count(&mut self, level_count: u8) {
        self.0.seek(LEVEL_COUNT_OFFSET);
        self.0.put_u8(level_count);
    }

    pub fn begin_write_level(&mut self) {
        self.0.seek(LEVEL_BEGIN_OFFSET);
    }

    pub fn write_level(&mut self, level: &LsmLevel) {
        self.0.put_u16(level.age);
        assert!(level.content.len() < u8::MAX as usize);
        self.0.put_u8(level.content.len() as u8);
        self.0.put_u8(0);

        for seg in &level.content {
            self.write_seg(seg);
        }
    }

    fn write_seg(&mut self, seg: &ImLsmSegment) {
        self.0.put_u64(seg.start_pid);
        self.0.put_u64(seg.end_pid);
        self.0.put_u64(seg.segments.len() as u64);
    }
}
