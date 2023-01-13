use serde::{Serialize, Deserialize};

#[derive(Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum TransactionType {
    Read,
    Write,
}

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum TransactionState {
    NoTrans,
    User,
    UserAuto,
    DbAuto,
}
