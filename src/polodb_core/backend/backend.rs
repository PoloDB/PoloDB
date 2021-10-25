use crate::DbResult;
use crate::page::RawPage;
use crate::transaction::TransactionType;

#[derive(Debug, Copy, Clone)]
pub(crate) struct AutoStartResult {
    pub auto_start: bool,
}

pub(crate) trait Backend {
    fn read_page(&self, page_id: u32) -> DbResult<RawPage>;
    fn write_page(&mut self, page: &RawPage) -> DbResult<()>;
    fn commit(&mut self) -> DbResult<()>;
    fn db_size(&self) -> u64;
    fn set_db_size(&mut self, size: u64) -> DbResult<()>;
    fn transaction_type(&self) -> Option<TransactionType>;
    fn upgrade_read_transaction_to_write(&mut self) -> DbResult<()>;
    fn rollback(&mut self) -> DbResult<()>;
    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()>;
}
