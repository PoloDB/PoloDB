#[cfg(feature = "redb")]
pub(crate) mod redb {
    use super::Transaction;
    use crate::errors::Error;
    use redb::{ReadTransaction, Savepoint, WriteTransaction};

    pub(crate) enum TransactionStates {
        Read(ReadTransaction),
        Write(WriteTransaction),
    }

    pub struct ReDBTransaction(pub(crate) TransactionStates);

    impl Transaction for ReDBTransaction {
        fn commit(self) -> crate::Result<()> {
            let TransactionStates::Write(transaction) = self.0 else {
                return Err(crate::Error::DbNotReady);
            };
            transaction.commit().map_err(|_| Error::DbNotReady)?;
            Ok(())
        }

        fn rollback(self) -> crate::Result<()> {
            let TransactionStates::Write(transaction) = self.0 else {
                return Err(Error::DbNotReady);
            };
            transaction.abort().map_err(|_| Error::DbNotReady)?;
            Ok(())
        }
    }
}
mod transaction;
mod transaction_inner;

#[cfg(feature = "redb")]
use crate::db::Result;
#[cfg(not(feature = "redb"))]
pub use transaction::Transaction;
pub(crate) use transaction_inner::TransactionInner;

#[cfg(feature = "redb")]
pub trait Transaction {
    fn commit(self) -> Result<()>;
    fn rollback(self) -> Result<()>;
}
