/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Copy, Clone)]
pub(crate) struct GlobalVariable(u32);

impl GlobalVariable {

    pub(crate) fn new(pos: u32) -> GlobalVariable {
        GlobalVariable(pos)
    }

    #[inline]
    pub(crate) fn pos(&self) -> u32 {
        self.0
    }

}

pub(crate) struct GlobalVariableSlot {
    pub pos: u32,
    pub name: Option<Box<str>>,
}
