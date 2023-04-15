/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use crate::DbResult;
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::mem_table::MemTable;

#[derive(Debug)]
pub(crate) enum LogCommand {
    Insert(Arc<[u8]>, Arc<[u8]>),
    Delete(Arc<[u8]>)
}

#[allow(dead_code)]
pub(crate) mod format {
    pub const EOF: u8     = 0x00;
    pub const PAD1: u8    = 0x01;
    pub const PAD2: u8    = 0x02;
    pub const COMMIT: u8  = 0x03;
    pub const JUMP: u8    = 0x04;
    pub const WRITE: u8   = 0x06;
    pub const DELETE: u8  = 0x08;
}

#[allow(dead_code)]
pub(crate) struct LsmCommitResult {
    pub offset: u64,
}

pub(crate) trait LsmLog: Send + Sync {

    fn start_transaction(&self) -> DbResult<()>;

    fn commit(&self, buffer: Option<&[u8]>) -> DbResult<LsmCommitResult>;

    fn update_mem_table_with_latest_log(
        &self,
        snapshot: &LsmSnapshot,
        mem_table: &mut MemTable,
    ) -> DbResult<()>;

    fn shrink(&self, snapshot: &mut LsmSnapshot) -> DbResult<()>;

    /// Sometimes we need to clear the log
    /// when the database is closing.
    ///
    /// But the log trait don't know if the database
    /// has sync all the data.
    /// If the data is not fully synced, it's not safe
    /// to clean the log.
    /// Otherwise, the log can be erased safely.
    fn enable_safe_clear(&self);

}

pub(crate) mod lsm_log_utils {
    use std::io::Read;
    use crc64fast::Digest;
    use crate::DbResult;
    use crate::lsm::lsm_backend::lsm_log::{format, LogCommand};
    use crate::lsm::mem_table::MemTable;
    use crate::utils::vli;

    pub(crate) fn flush_commands_to_mem_table(commands: Vec<LogCommand>, mem_table: &mut MemTable) {
        for cmd in commands {
            match cmd {
                LogCommand::Insert(key, value) => {
                    mem_table.put(key, value, true);
                }
                LogCommand::Delete(key) => {
                    mem_table.delete(key.as_ref(), true);
                }
            }
        }
    }

    pub(crate) fn read_write_command(mmap: &[u8], commands: &mut Vec<LogCommand>, ptr: &mut usize) -> DbResult<()> {
        let mut remain = &mmap[*ptr..];

        let key_len = vli::decode_u64(&mut remain)?;
        let mut key_buff = vec![0u8; key_len as usize];
        remain.read_exact(&mut key_buff)?;

        let value_len = vli::decode_u64(&mut remain)?;
        let mut value_buff = vec![0u8; value_len as usize];
        remain.read_exact(&mut value_buff)?;

        commands.push(LogCommand::Insert(key_buff.into(), value_buff.into()));

        *ptr = remain.as_ptr() as usize - mmap.as_ptr() as usize;

        Ok(())
    }

    pub(crate) fn read_delete_command(mmap: &[u8], commands: &mut Vec<LogCommand>, ptr: &mut usize) -> DbResult<()> {
        let mut remain = &mmap[*ptr..];

        let key_len = vli::decode_u64(&mut remain)?;
        let mut key_buff = vec![0u8; key_len as usize];
        remain.read_exact(&mut key_buff)?;

        commands.push(LogCommand::Delete(key_buff.into()));

        *ptr = remain.as_ptr() as usize - mmap.as_ptr() as usize;

        Ok(())
    }

    fn crc64(bytes: &[u8]) -> u64 {
        let mut c = Digest::new();
        c.write(bytes);
        c.sum64()
    }

    pub(crate) fn update_mem_table_by_buffer(
        content: &[u8],
        mut start_offset: usize,
        mem_table: &mut MemTable,
        flush_remain: bool,
    ) -> (usize, bool) {
        let mut ptr = start_offset;
        let mut reset = false;
        let mut commands: Vec<LogCommand> = vec![];

        while ptr < content.len() {
            let flag = content[ptr];
            ptr += 1;

            if flag == format::COMMIT {
                let checksum = crc64(&content[start_offset..(ptr - 1)]);

                if ptr + 8 > content.len() {
                    reset = true;
                    break;
                }
                let mut checksum_be: [u8; 8] = [0; 8];
                checksum_be.copy_from_slice(&content[ptr..(ptr + 8)]);
                let expect_checksum = u64::from_be_bytes(checksum_be);
                ptr += 8;

                if checksum != expect_checksum {
                    reset = true;
                    break;
                }

                start_offset = ptr;

                flush_commands_to_mem_table(commands, mem_table);
                commands = vec![];
            } else if flag == format::WRITE {
                let test_write = read_write_command(content, &mut commands, &mut ptr);
                if test_write.is_err() {
                    reset = true;
                    break;
                }
            } else if flag == format::DELETE {
                let test_delete = read_delete_command(
                   content,
                    &mut commands,
                    &mut ptr,
                );
                if test_delete.is_err() {
                    reset = true;
                    break;
                }
            } else {  // unknown command
                reset = true;
                break;
            }
        }

        if flush_remain {
            flush_commands_to_mem_table(commands, mem_table);
            commands = vec![];
        }

        (start_offset, reset)
    }

}
