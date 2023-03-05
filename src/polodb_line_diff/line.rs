/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub struct Line {
    index: usize,
    content: String,
}

impl Line {

    pub fn new(index: usize, content: String) -> Line {
        Line {
            index,
            content,
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn content(&self) -> &str {
        self.content.as_str()
    }

}
