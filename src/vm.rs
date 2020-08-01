use super::bson::value;
use std::vec::Vec;

struct VM {
    pc: i32,
    st: i32,
    stack: Vec<value::Value>,
}

impl VM {

    fn new() -> VM {
        VM {
            pc: 0,
            st: 0,
            stack: Vec::new(),
        }
    }

}
