use bson::oid::ObjectId;
use crate::DbResult;
use crate::page::RawPage;

pub(crate) trait Session {
    fn pipeline_read_page(&mut self, page_id: u32) -> DbResult<RawPage>;
    fn pipeline_write_page(&mut self, page: &RawPage) -> DbResult<()>;
}

pub(crate) struct DefaultSession {
    _id: ObjectId,
}

impl DefaultSession {

    pub(crate) fn new(id: ObjectId) -> DefaultSession {
        DefaultSession {
            _id: id,
        }
    }

}

impl Session for DefaultSession {
    fn pipeline_read_page(&mut self, _page_id: u32) -> DbResult<RawPage> {
        todo!()
    }

    fn pipeline_write_page(&mut self, _page: &RawPage) -> DbResult<()> {
        todo!()
    }
}
