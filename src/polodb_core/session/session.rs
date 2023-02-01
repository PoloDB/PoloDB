use std::cell::Cell;
use std::cmp::min;
use std::num::NonZeroU32;
use bson::Document;
use crate::data_ticket::DataTicket;
use crate::{DbErr, DbResult, TransactionType};
use crate::backend::AutoStartResult;
use crate::page::data_page_wrapper::DataPageWrapper;
use crate::page::header_page_wrapper::HeaderPageWrapper;
use crate::page::large_data_page_wrapper::LargeDataPageWrapper;
use crate::page::{FreeListDataWrapper, RawPage};

pub(crate) trait Session {
    fn pipeline_read_page(&self, page_id: u32) -> DbResult<RawPage>;
    fn pipeline_write_page(&self, page: &RawPage) -> DbResult<()>;
    fn page_size(&self) -> NonZeroU32;
    fn store_doc(&self, doc: &Document) -> DbResult<DataTicket>;
    fn alloc_page_id(&self) -> DbResult<u32>;
    fn free_pages(&self, pages: &[u32]) -> DbResult<()>;
    fn free_page(&self, pid: u32) -> DbResult<()> {
        self.free_pages(&[pid])
    }
    fn free_data_ticket(&self, data_ticket: &DataTicket) -> DbResult<Vec<u8>>;
    fn get_doc_from_ticket(&self, data_ticket: &DataTicket) -> DbResult<Option<Document>>;
    fn auto_start_transaction(&self, ty: TransactionType) -> DbResult<AutoStartResult>;
    fn auto_commit(&self) -> DbResult<()>;
    fn auto_rollback(&self) -> DbResult<()>;
    fn start_transaction(&self, ty: TransactionType) -> DbResult<()>;
    fn commit(&self) -> DbResult<()>;
    fn rollback(&self) -> DbResult<()>;
}

pub(crate) trait SessionInner {
    fn pipeline_read_page(&mut self, page_id: u32) -> DbResult<RawPage>;
    fn pipeline_write_page(&mut self, page: &RawPage) -> DbResult<()>;
    fn distribute_data_page_wrapper(&mut self, data_size: u32) -> DbResult<DataPageWrapper>;
    fn return_data_page_wrapper(&mut self, wrapper: DataPageWrapper);
    fn actual_alloc_page_id(&mut self) -> DbResult<u32>;
    fn free_pages(&mut self, pages: &[u32]) -> DbResult<()>;

    fn free_page(&mut self, pid: u32) -> DbResult<()> {
        self.free_pages(&[pid])
    }

    fn page_size(&self) -> NonZeroU32;

    fn alloc_page_id(&mut self) -> DbResult<u32> where Self: Sized {
        let page_id = match try_get_free_page_id(self)? {
            Some(page_id) =>  {
                self.pipeline_write_null_page(page_id)?;

                crate::polo_log!("get new page_id from free list: {}", page_id);

                Ok(page_id)
            }

            None =>  {
                self.actual_alloc_page_id()
            }
        }?;

        Ok(page_id)
    }

    fn store_doc(&mut self, doc: &Document) -> DbResult<DataTicket> where Self: Sized {
        let mut bytes = Vec::with_capacity(512);
        crate::doc_serializer::serialize(doc, &mut bytes)?;

        if bytes.len() >= self.page_size().get() as usize / 2 {
            return store_large_data(self, &bytes);
        }

        let mut wrapper = self.distribute_data_page_wrapper(bytes.len() as u32)?;
        let index = wrapper.bar_len() as u16;
        let pid = wrapper.pid();
        if (wrapper.remain_size() as usize) < bytes.len() {
            panic!("page size not enough: {}, bytes: {}", wrapper.remain_size(), bytes.len());
        }
        wrapper.put(&bytes);

        self.pipeline_write_page(wrapper.borrow_page())?;

        self.return_data_page_wrapper(wrapper);

        Ok(DataTicket {
            pid,
            index,
        })
    }

    fn pipeline_write_null_page(&mut self, page_id: u32) -> DbResult<()> {
        let page = RawPage::new(page_id, self.page_size());
        self.pipeline_write_page(&page)
    }

    fn get_first_page(&mut self) -> Result<RawPage, DbErr> {
        self.pipeline_read_page(0)
    }

    fn free_data_ticket(&mut self, data_ticket: &DataTicket) -> DbResult<Vec<u8>> where Self: Sized {
        crate::polo_log!("free data ticket: {}", data_ticket);

        if data_ticket.is_large_data() {
            return free_large_data_page(self, data_ticket.pid);
        }

        let page = self.pipeline_read_page(data_ticket.pid)?;
        let mut wrapper = DataPageWrapper::from_raw(page);
        let bytes = wrapper.get(data_ticket.index as u32).unwrap().to_vec();
        wrapper.remove(data_ticket.index as u32);
        if wrapper.is_empty() {
            self.free_page(data_ticket.pid)?;
        }
        let page = wrapper.consume_page();
        self.pipeline_write_page(&page)?;
        Ok(bytes)
    }

    fn get_doc_from_ticket(&mut self, data_ticket: &DataTicket) -> DbResult<Option<Document>> {
        if data_ticket.is_large_data() {
            return self.get_doc_from_large_page(data_ticket.pid);
        }
        let page = self.pipeline_read_page(data_ticket.pid)?;
        let wrapper = DataPageWrapper::from_raw(page);
        let bytes = wrapper.get(data_ticket.index as u32);
        if let Some(bytes) = bytes {
            let mut my_ref: &[u8] = bytes;
            let bytes: &mut &[u8] = &mut my_ref;
            let doc = crate::doc_serializer::deserialize(bytes)?;
            return Ok(Some(doc));
        }
        Ok(None)
    }

    fn get_doc_from_large_page(&mut self, pid: u32) -> DbResult<Option<Document>> {
        let mut bytes: Vec<u8> = Vec::with_capacity(self.page_size().get() as usize);

        let mut next_pid = pid;

        while next_pid != 0 {
            let page = self.pipeline_read_page(next_pid)?;
            let wrapper = LargeDataPageWrapper::from_raw(page);
            wrapper.write_to_buffer(&mut bytes);
            next_pid = wrapper.next_pid();
        }

        let mut my_ref: &[u8] = bytes.as_ref();
        let doc = crate::doc_serializer::deserialize(&mut my_ref)?;
        Ok(Some(doc))
    }
}

fn free_large_data_page(session: &mut impl SessionInner, pid: u32) -> DbResult<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(session.page_size().get() as usize);
    let mut free_pid: Vec<u32> = Vec::new();

    let mut next_pid = pid;
    while next_pid != 0 {
        free_pid.push(next_pid);

        let page = session.pipeline_read_page(next_pid)?;
        let wrapper = LargeDataPageWrapper::from_raw(page);
        wrapper.write_to_buffer(&mut result);
        next_pid = wrapper.next_pid();
    }

    session.free_pages(&free_pid)?;
    Ok(result)
}

fn get_free_page_id_from_external_page(session: &mut impl SessionInner, free_list_page_id: u32, free_and_next: &Cell<i64>) -> DbResult<u32> {
    let raw_page = session.pipeline_read_page(free_list_page_id)?;
    let mut free_list_page_wrapper = FreeListDataWrapper::from_raw(raw_page);
    let pid = free_list_page_wrapper.consume_a_free_page();
    if free_list_page_wrapper.size() == 0 {
        let next_pid = free_list_page_wrapper.next_pid();
        session.free_page(pid)?;
        free_and_next.set(next_pid as i64);
    } else {
        session.pipeline_write_page(free_list_page_wrapper.borrow_page())?;
        free_and_next.set(-1);
    }
    Ok(pid)
}

fn try_get_free_page_id(session: &mut impl SessionInner) -> DbResult<Option<u32>> {
    let first_page = session.get_first_page()?;
    let mut first_page_wrapper = HeaderPageWrapper::from_raw_page(first_page);

    let free_list_page_id = first_page_wrapper.get_free_list_page_id();
    if free_list_page_id != 0 {
        let free_and_next: Cell<i64> = Cell::new(-1);
        let pid = get_free_page_id_from_external_page(session, free_list_page_id, &free_and_next)?;
        if free_and_next.get() >= 0 {
            first_page_wrapper.set_free_list_page_id(free_and_next.get() as u32);
            session.pipeline_write_page(&first_page_wrapper.0)?;
        }
        return Ok(Some(pid));
    }

    let free_list_size = first_page_wrapper.get_free_list_size();
    if free_list_size == 0 {
        return Ok(None);
    }

    let result = first_page_wrapper.get_free_list_content(free_list_size - 1);
    first_page_wrapper.set_free_list_size(free_list_size - 1);

    session.pipeline_write_page(&first_page_wrapper.0)?;

    Ok(Some(result))
}


fn store_large_data(session: &mut impl SessionInner, bytes: &Vec<u8>) -> DbResult<DataTicket> {
    let mut remain: i64 = bytes.len() as i64;
    let mut pages: Vec<LargeDataPageWrapper> = Vec::new();
    while remain > 0 {
        let new_id = session.alloc_page_id()?;
        let mut large_page_wrapper = LargeDataPageWrapper::init(
            new_id,
            session.page_size()
        );
        let max_cap = large_page_wrapper.max_data_cap() as usize;
        let start_index: usize = (bytes.len() as i64 - remain) as usize;
        let end_index: usize = min(start_index + max_cap, bytes.len());
        large_page_wrapper.put(&bytes[start_index..end_index]);

        if pages.len() > 0 {
            let prev = pages.last_mut().unwrap();
            prev.put_next_pid(new_id);
        }
        pages.push(large_page_wrapper);
        remain = (bytes.len() - end_index) as i64;
    }

    let first_pid = pages.first().unwrap().borrow_page().page_id;

    for page in pages {
        session.pipeline_write_page(page.borrow_page())?;
    }

    Ok(DataTicket::large_ticket(first_pid))
}
