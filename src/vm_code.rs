
#[repr(u8)]
pub enum VmCode {
    PushUndefined = 0,  // 1
    // PushNull,
    PushI32,            // 5
    PushI64,            // 9
    PushTrue,           // 1
    PushFalse,          // 1
    PushBool,           // 2
    Pop,                // 1
    CreateCollection,   // 1, st: -2
    // AddI32,
    // AddI64,
    // Add,
    // SubI32,
    // SubI64,
    // Sub,
    // MulI32,
    // MulI64,
    // Mul,
    // DivI32,
    // DivI64,
    // Div,
    // Mod,
    Resolve,             // 1
    Reject               // 1
}
