/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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
