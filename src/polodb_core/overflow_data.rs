use std::rc::Rc;
use std::cell::RefCell;
use crate::page::{RawPage, PageHandler};
use crate::db::DbResult;
use crate::error::DbErr;

static OVERFLOW_PAGE_HEADER_SIZE: u32 = 16;

pub struct OverflowDataTicketItem {
    pub page_id:   u32,
    pub offset:    u16,
    pub cap:       u32,
}

// spaces to store overflow data
pub struct OverflowDataTicket {
    pub items: Vec<OverflowDataTicketItem>,
}

// Offset 0: magic(2 bytes)
// Offset 2: page type(2 bytes)
// Offset 16: data begin
pub struct OverflowDataWrapper {
    ctx:        Rc<RefCell<PageHandler>>,
    record_bar: Vec<u16>,
    page:       RawPage,
}

impl OverflowDataWrapper {

    pub(crate) fn from_raw_page(ctx: Rc<RefCell<PageHandler>>, page: RawPage) -> DbResult<OverflowDataWrapper> {
        let mut page = page;

        // init
        if page.data[0] != 0xFF || page.data[1] != 0x67 {
            page.data[0] = 0xFF;
            page.data[1] = 0x67;
        }

        let mut record_bar = vec![];
        let mut index: u32 = OVERFLOW_PAGE_HEADER_SIZE;
        let mut tmp: u16 = page.get_u16(index);
        while tmp != 0 {
            record_bar.push(tmp);

            index += 2;
            tmp = page.get_u16(index);
        }

        Ok(OverflowDataWrapper {
            ctx,
            record_bar,
            page,
        })
    }

    pub fn alloc(&mut self, size: u32) -> DbResult<OverflowDataTicketItem> {
        let ctx = self.ctx.borrow();
        let remain_space: i64 = (ctx.page_size as i64) - (OVERFLOW_PAGE_HEADER_SIZE as i64) - (((self.record_bar.len() as i64) + 2) * 2);

        if remain_space < 32 {
            return Err(DbErr::PageSpaceNotEnough);
        }

        if (remain_space - 8) <= (size as i64) {  // page can be stored in this page
            let last_bar: u32 = match self.record_bar.last() {
                Some(i) => (*i) as u32,
                None => ctx.page_size,
            };

            let bar = (last_bar - size) as u16;

            self.record_bar.push(bar);

            let item = OverflowDataTicketItem {
                page_id: self.page.page_id,
                offset: bar,
                cap: size,
            };

            return Ok(item);
        }

        Err(DbErr::NotImplement)
    }

}
