use super::bson::value;
use super::bson::value::Value;
use super::vm_code::{ VmCode };
use std::vec::Vec;

static STACK_SIZE: usize = 256;

pub struct SubProgram {
    static_values:    Vec<Value>,
    instructions:     Vec<u8>,
}

pub enum VmState {
    Reject = -1,
    Init = 0,
    Running = 1,
    Resolve = 2,
}

pub struct VM {
    state: VmState,
    stack: Vec<value::Value>,
    program: Box<SubProgram>,
}

impl VM {

    fn new(program: Box<SubProgram>) -> VM {
        let mut stack = Vec::new();
        stack.resize(STACK_SIZE, value::Value::Undefined);
        VM {
            state: VmState::Init,
            stack,
            program,
        }
    }

    fn execute(&mut self) {
        // let pc: *mut u8 = self.pro
        unsafe {
            let mut pc: *const u8 = self.program.instructions.as_ptr();
            let mut st: *mut Value = self.stack.as_mut_ptr();
            loop {
                let op: VmCode = (pc.cast() as *const VmCode).read();
                pc = pc.add(1);

                match op {
                    VmCode::PushUndefined => {
                        st.write(Value::Undefined);
                        st = st.add(1);
                        pc = pc.add(1);
                    }

                    VmCode::PushI32 => {
                        let mut buffer: [u8; 4] = [0; 4];
                        pc.copy_to(buffer.as_mut_ptr(), 4);
                        let num = i32::from_be_bytes(buffer);
                        st.write(Value::Int(num as i64));
                        st = st.add(1);
                        pc = pc.add(4);
                    }

                    VmCode::PushI64 => {
                        let mut buffer: [u8; 8] = [0; 8];
                        pc.copy_to(buffer.as_mut_ptr(), 8);
                        let num = i64::from_be_bytes(buffer);
                        st.write(Value::Int(num));
                        st = st.add(1);
                        pc = pc.add(8);
                    }

                    VmCode::PushTrue => {
                        st.write(Value::Boolean(true));
                        st = st.add(1);
                    }

                    VmCode::PushFalse => {
                        st.write(Value::Boolean(false));
                        st = st.add(1);
                    }

                    VmCode::PushBool => {
                        let value = pc.read() != 0;
                        st.write(Value::Boolean(value));
                        st = st.add(1);
                        pc = pc.add(1);
                    }

                    VmCode::Pop => {
                        st = st.sub(1);
                    }

                    VmCode::CreateCollection => {
                        let n1 = st.sub(1);
                        let option = n1.read();
                        let n2 = st.sub(2);
                        let name = n2.read();
                        st = n2;

                        println!("create collection: {}, {}", name, name)
                    }

                    // VmCode::AddI32 => {
                    //     let to_add = inst.op1 as i32;
                    //     match self.stack[self.st - 1] {
                    //         Value::I32(current) =>
                    //             self.state[self.st - 1] = Value::I32(current + to_add),
                    //
                    //         _ => ()
                    //
                    //     }
                    // }

                    VmCode::Resolve => {
                        self.state = VmState::Resolve;
                        break
                    }

                    VmCode::Reject => {
                        self.state = VmState::Reject;
                        break
                    }
                }
            }
        }
    }

}
