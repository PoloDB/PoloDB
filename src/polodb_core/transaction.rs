use serde::{Serialize, Deserialize};

#[derive(Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
#[repr(u8)]
pub enum TransactionType {
    Read = 1,
    Write = 2,
}

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum TransactionState {
    NoTrans,
    User,
    UserAuto,
    DbAuto,
}
