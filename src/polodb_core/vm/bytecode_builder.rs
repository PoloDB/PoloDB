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
use crate::bson::Value;

const INIT_BUFFER_SIZE: usize = 512;
const BYTECODE_MAGIC: [u8; 2] = [0x06, 0x07];
const STATIC_AREA_BEGIN_MAGIC: [u8; 2] = [0x06, 0x08];
const DIVIDER_MAGIC: [u8; 2] = ['\r' as u8, '\n' as u8];
const STATIC_VEC_DEFAULT_SIZE: usize = 32;

pub struct ByteCodeBuilder {
    static_values: Vec<Value>,
    buffer: Vec<u8>,
}

impl ByteCodeBuilder {

    pub fn new() -> ByteCodeBuilder {
        let buffer = Vec::with_capacity(INIT_BUFFER_SIZE);
        let mut result = ByteCodeBuilder {
            static_values: Vec::with_capacity(STATIC_VEC_DEFAULT_SIZE),
            buffer,
        };

        result.put(&BYTECODE_MAGIC);
        result
    }

    pub fn add_static_values(&mut self, value: Value) -> usize {
        let result = self.static_values.len();
        self.static_values.push(value);
        result
    }

    pub unsafe fn put_raw(&mut self, mut bytes: *mut u8, size: usize) {
        for _ in 0..size {
            self.buffer.push(bytes.read());
            bytes = bytes.add(1);
        }
    }

    #[inline]
    pub fn put(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    #[inline]
    pub fn add_divider(&mut self) {
        self.put(&DIVIDER_MAGIC);
    }

    #[inline]
    pub fn finish(&mut self) {
        self.buffer.push(0);
    }

}
