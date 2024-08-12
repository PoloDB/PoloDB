/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod lsm_file_backend;
pub(crate) mod lsm_log;
mod file_writer;
mod snapshot_reader;
mod lsm_backend;
mod lsm_file_log;

pub(crate) use lsm_file_backend::LsmFileBackend;

pub(crate) use lsm_file_log::LsmFileLog;

pub(crate) use lsm_log::LsmLog;
pub(crate) use lsm_backend::LsmBackend;

#[allow(unused)]
pub(crate) mod format {
    pub const LSM_START_DELETE: u8 = 0x01;
    pub const LSM_END_DELETE: u8   = 0x02;
    pub const LSM_POINT_DELETE: u8 = 0x03;
    pub const LSM_INSERT: u8       = 0x04;
    pub const LSM_SEPARATOR: u8    = 0x10;
    pub const LSM_SYSTEMKEY: u8    = 0x20;
}
