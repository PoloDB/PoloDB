use crate::DbResult;
use crate::transaction::TransactionType;

#[derive(Debug, Copy, Clone)]
pub(crate) struct AutoStartResult {
    pub auto_start: bool,
}

pub(crate) trait Backend {
    fn auto_start_transaction(&mut self, ty: TransactionType) -> DbResult<AutoStartResult>;
    fn auto_rollback(&mut self) -> DbResult<()>;
    fn auto_commit(&mut self) -> DbResult<()>;
    fn start_transaction(&mut self, ty: TransactionType) -> DbResult<()>;
    fn commit(&mut self) -> DbResult<()>;
    fn rollback(&mut self) -> DbResult<()>;
}
