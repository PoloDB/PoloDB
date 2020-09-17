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

// 6 bytes in store
#[derive(Clone)]
pub(crate) struct DataTicket {
    pub pid: u32,
    pub index: u16,
}

impl DataTicket {

    pub fn to_bytes(&self) -> [u8; 6] {
        let mut result = [0; 6];

        let pid_bytes = self.pid.to_be_bytes();
        let index_bytes = self.index.to_be_bytes();

        result[0..4].copy_from_slice(&pid_bytes);
        result[4..6].copy_from_slice(&index_bytes);

        result
    }

    pub fn from_bytes(bytes: &[u8]) -> DataTicket {
        let mut pid_bytes = [0; 4];
        let mut index_bytes = [0; 2];

        pid_bytes.copy_from_slice(&bytes[0..4]);
        index_bytes.copy_from_slice(&bytes[4..6]);

        let pid = u32::from_be_bytes(pid_bytes);
        let index = u16::from_be_bytes(index_bytes);

        DataTicket { pid, index }
    }

}
