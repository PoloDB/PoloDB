// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq)]
#[allow(dead_code)]
pub enum DbOp {
    _EOF = 0,

    // label
    //
    // 5 bytes
    // op1. label id
    Label = 1,

    // increase the value on the top of the stack by 1
    Inc,

    IncR2,

    // reset the pc to the position of op0
    //
    // 5 bytes
    // op1. location: 4 bytes
    Goto,

    // if r0 is true, jump to location
    //
    // 5 bytes
    // op1. location: 4 bytes
    IfTrue,

    // if r0 is false, jump to location
    //
    // 5 bytes
    // op1. location: 4 bytes
    IfFalse,

    // reset the cursor to the first element
    // if empty, jump to location
    //
    // 5 bytes
    // op1. location: 4 bytes
    Rewind,

    // reset the cursor pointer to the element
    // in btree by the primary key on the top of the stack
    // if the item can not be found, jump to the location
    //
    // 5 bytes
    // op1. location: 4 bytes
    FindByPrimaryKey,

    // reset the cursor pointer to the element
    //
    // 5 bytes
    // op1. location: 4 bytes
    FindByIndex,

    // next element of the cursor
    // if no next element, pass
    // otherwise, jump to location
    //
    // push current value to the stack
    //
    // 5 bytes
    // op1. location: 4bytes
    Next,

    // next index value
    // advance the cursor to next index
    // push the value of the index on the top of the stack
    //
    // if no next element, pass
    // otherwise, jump to location
    //
    // push current value to the stack
    //
    // 5 bytes
    // op1. location: 4bytes
    NextIndexValue,

    // push value to the stack
    //
    // 5 bytes
    // op1. value_index: 4bytes
    PushValue,

    // 1 byte
    PushTrue,

    // 1 byte
    PushFalse,

    // 1byte
    PushDocument,

    // push r0 to the top of the stack
    //
    // 1 byte
    PushR0,

    // store the top of the stack to r0
    //
    // 1 byte
    StoreR0,

    // get the field of top of the stack
    // push the value to the stack
    //
    // if failed, goto op2
    //
    // 9 bytes
    // op1. value_index: 4bytes
    // op2. location: 4bytes
    GetField,

    // remove the field
    //
    // 5 bytes
    // op1. value_index: 4bytes
    UnsetField,

    // increment the field
    // if not exists, set the value
    //
    // throw error if field is null
    //
    // top-1 is the value to push
    // top-2 is the doc to change
    //
    // 5 bytes
    // op1. field_name_index: 4bytes
    IncField,

    // multiple the field
    // if not exists, set the value
    //
    // throw error if field is null
    //
    // top-1 is the value to push
    // top-2 is the doc to change
    //
    // 5 bytes
    // op1. field_name_index: 4bytes
    MulField,

    // set the value of the field
    //
    // top-1 is the value to push
    // top-2 is the doc to change
    //
    // 5 bytes
    // op1. field_name_index: 4bytes
    SetField,

    // get the size of array
    // push to the top of the stack
    //
    // 1 byte
    ArraySize,

    // push an element to the array
    //
    // 1 byte
    ArrayPush,

    ArrayPopFirst,
    ArrayPopLast,

    // update current item on cursor
    //
    // 1 byte
    UpdateCurrent,

    // delete current item on cursor
    //
    // 1 byte
    DeleteCurrent,

    // insert the index of the top value on the stack
    //
    // top-1 is the value
    //
    // 5 byte
    // op1. index info id: 4 bytes
    InsertIndex,

    // delete the index of the top value on the stack
    //
    // top-1 is the value
    //
    // 5 byte
    // op1. index info id: 4 bytes
    DeleteIndex,

    // duplicate the top of the stack
    Dup,

    Pop,

    // 5 bytes
    //
    // count: pop offset count
    Pop2,

    // check if top 2 values on the stack are qual
    // the result is stored in r0
    //
    // -1 for not comparable
    // 0 false not equal
    // 1 for equal
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    Regex,

    Not,

    // check if top0 is in top2
    // the result is stored in r0
    In,

    // open a cursor with op0 as root_pid
    //
    // 5 bytes
    // op1. prefix_id: 4 bytes
    OpenRead,

    // open a cursor with op0 as root_pid
    //
    // 5 bytes
    // op1. prefix_id: 4 bytes
    OpenWrite,

    // Pause the db
    // The top value of the stack
    // is the result
    ResultRow,

    // Close cursor
    Close,

    SaveStackPos,

    RecoverStackPos,

    // call a method
    //
    // 9 bytes
    // op1. location: 4 bytes
    // op2. size of params: 4 bytes
    Call,

    // call a method
    //
    // 9 bytes
    // op1. func id: 4 bytes
    // op2. size of params: 4 bytes
    CallExternal,

    // return from a method with 0 size
    //
    // 1 byte
    Ret0,

    // return from a method
    //
    // 5 bytes
    // op1. return value size: 4 bytes
    Ret,

    // if r0 is false, return
    //
    // 5 bytes
    // op1. return value size: 4 bytes
    IfFalseRet,

    // load global variable on the stack
    //
    // 5 bytes
    // op1. global variable id: 4 bytes
    LoadGlobal,

    // store global variable from the stack
    // 5 bytes
    // op1. global variable id: 4 bytes
    StoreGlobal,

    // Exit
    // Close cursor automatically
    Halt,

}
