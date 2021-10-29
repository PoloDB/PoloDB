use num_enum::TryFromPrimitive;

#[derive(Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(i32)]
pub enum MsgTy {
    Undefined = 0,
    Find = 1,
}
