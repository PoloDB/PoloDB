use num_enum::TryFromPrimitive;

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, Clone, Copy)]
#[repr(i32)]
pub enum MsgTy {
    Undefined = 0,
    Find = 1,
    FindOne = 2,
    SafelyQuit = 255,
}
