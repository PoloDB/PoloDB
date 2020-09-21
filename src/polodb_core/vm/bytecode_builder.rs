/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */
const INIT_BUFFER_SIZE: usize = 512;
const BYTECODE_MAGIC: [u8; 2] = [0x06, 0x07];

pub struct ByteCodeBuilder {
    buffer: Vec<u8>,
}

impl ByteCodeBuilder {

    pub fn new() -> ByteCodeBuilder {
        let buffer = Vec::with_capacity(INIT_BUFFER_SIZE);
        let mut result = ByteCodeBuilder {
            buffer,
        };

        result.put(&BYTECODE_MAGIC);
        result
    }

    #[inline]
    pub fn put(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

}
