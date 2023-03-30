use crate::lsm::mem_table::MemTable;

pub(crate) struct LsmSessionInner {
    pub(crate) mem_table: MemTable,
}

impl LsmSessionInner {

    pub(crate) fn new() -> LsmSessionInner {
        LsmSessionInner {
            mem_table: MemTable::new(),
        }
    }

}
