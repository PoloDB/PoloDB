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

#[repr(u8)]
pub enum VmCode {
    PushNull = 0,       // 1
    // PushNull,
    PushI32,            // 5
    PushI64,            // 9
    PushTrue,           // 1
    PushFalse,          // 1
    PushBool,           // 2
    Pop,                // 1
    CreateCollection,   // 1, st: -2
    // AddI32,
    // AddI64,
    // Add,
    // SubI32,
    // SubI64,
    // Sub,
    // MulI32,
    // MulI64,
    // Mul,
    // DivI32,
    // DivI64,
    // Div,
    // Mod,
    Resolve,             // 1
    Reject               // 1
}
