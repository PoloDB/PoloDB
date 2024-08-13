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

#[derive(Copy, Clone)]
pub(crate) struct Label(u32);

impl Label {

    pub(crate) fn new(pos: u32) -> Label {
        Label(pos)
    }

    #[inline]
    pub(crate) fn pos(&self) -> u32 {
        self.0
    }

    #[inline]
    pub(crate) fn u_pos(&self) -> usize {
        self.0 as usize
    }

}

pub(crate) struct JumpTableRecord {
    pub(crate) begin_loc: u32,
    pub(crate) offset: u32,
    pub(crate) label_id: u32,
}

impl JumpTableRecord {

    pub(crate) fn new(begin_loc: u32, offset: u32, label_id: u32) -> JumpTableRecord {
        JumpTableRecord {
            begin_loc,
            offset,
            label_id,
        }
    }

}

pub(crate) enum LabelSlot {
    Empty,
    UnnamedLabel(u32),
    LabelWithString(u32, Box<str>),
}

impl LabelSlot {

    pub(crate) fn position(&self) -> u32 {
        match self {
            LabelSlot::Empty => unreachable!(),
            LabelSlot::UnnamedLabel(pos) => *pos,
            LabelSlot::LabelWithString(pos, _) => *pos,
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, LabelSlot::Empty)
    }

}
