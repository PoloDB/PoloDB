use crate::{db::Result, transaction::Transaction, Config};
use std::path::Path;

pub trait Backend
where
    Self: Sized,
{
    type Transaction: Transaction;

    fn try_open(path: &Path) -> Result<Self>;
    fn try_open_with_config(path: &Path, config: Config) -> Result<Self>;
    fn begin_transaction(&self) -> Result<Self::Transaction>;
}
