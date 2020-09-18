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
use crate::bson::Document;

// root_btree schema
// {
//   _id: String,
//   root_pid: Int,
//   flags: Int,
// }
//
// flags indicates:
// key_ty: 1byte
// ...
//
pub(crate) struct MetaDocEntry {
    pub id: String,
    pub root_pid: u32,
    flags: u32,
}

impl MetaDocEntry {

    pub(crate) fn from_doc(doc: &Document) -> MetaDocEntry {
        let id = doc.get(meta_doc_key::ID.into()).unwrap().unwrap_string();
        let root_pid = doc.get(meta_doc_key::ROOT_PID.into()).unwrap().unwrap_int();
        let flags = doc.get(meta_doc_key::FLAGS.into()).unwrap().unwrap_int();
        MetaDocEntry {
            id: id.into(),
            root_pid: root_pid as u32,
            flags: flags as u32,
        }
    }

    fn key_ty(&self) -> u8 {
        (self.flags & 0x01) as u8
    }

}

pub(crate) mod meta_doc_key {
    pub(crate) static ID: &str       = "_id";
    pub(crate) static ROOT_PID: &str = "root_pid";
    pub(crate) static FLAGS: &str    = "flags";
    pub(crate) static INDEXES: &str  = "indexes";

    pub(crate) mod index {
        pub(crate) static NAME: &str = "name";
        pub(crate) static V: &str    = "v";
        pub(crate) static UNIQUE: &str = "unique";
        pub(crate) static ROOT_PID: &str = "root_pid";

    }

}

