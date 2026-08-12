use crate::{
    db::{rocksdb_wrapper::RocksDBWrapper, RocksDBTransaction},
    Result,
};
use std::path::Path;

pub trait Backend
where
    Self: Sized,
{
    type ReadTransaction;
    type WriteTransaction;

    fn transaction(&self) -> Result<Self::ReadTransaction>;

    fn write_transaction(&self) -> Result<Self::WriteTransaction>;

    fn open_path<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>;
}

impl Backend for RocksDBWrapper {
    type ReadTransaction = RocksDBTransaction;
    type WriteTransaction = RocksDBTransaction;

    fn open_path<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        Ok(Self::open(path.as_ref())?)
    }

    fn write_transaction(&self) -> Result<Self::WriteTransaction> {
        Ok(self.begin_transaction()?)
    }

    fn transaction(&self) -> Result<Self::ReadTransaction> {
        todo!()
    }
}
