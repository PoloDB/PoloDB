
pub enum VmCode {
    Undefined,
    PushUndefined,
    PushNull,
    PushI32,
    PushI64,
    PushTrue,
    PushFalse,
    PushBool,
    Pop,
    AddI32,
    AddI64,
    Add,
    SubI32,
    SubI64,
    Sub,
    MulI32,
    MulI64,
    Mul,
    DivI32,
    DivI64,
    Div,
    Mod,
    Resolve,
    Reject
}

pub struct Inst {
    op: VmCode,
    op1: u32,
    op2: u32,
    op3: u32,
}
