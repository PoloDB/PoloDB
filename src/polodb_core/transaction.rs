
#[derive(Eq, PartialEq, Copy, Clone)]
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
