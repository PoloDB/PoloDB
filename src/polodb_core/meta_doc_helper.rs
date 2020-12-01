use polodb_bson::{Document, Value};
use crate::DbResult;
use crate::error::DbErr;

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
    pub id: u32,
    pub name: String,
    pub root_pid: u32,
    flags: u32,
}

pub(crate) const KEY_TY_FLAG: u32 = 0b11111111;

impl MetaDocEntry {

    pub fn new(id: u32, name: String, root_pid: u32) -> MetaDocEntry {
        MetaDocEntry {
            id,
            name,
            root_pid,
            flags: 0
        }
    }

    pub(crate) fn from_doc(doc: &Document) -> MetaDocEntry {
        let id = doc.get(meta_doc_key::ID).unwrap().unwrap_int() as u32;
        let name = doc.get(meta_doc_key::NAME).unwrap().unwrap_string();
        let root_pid = doc.get(meta_doc_key::ROOT_PID).unwrap().unwrap_int();
        let flags = doc.get(meta_doc_key::FLAGS).unwrap().unwrap_int();
        MetaDocEntry {
            id,
            name: name.into(),
            root_pid: root_pid as u32,
            flags: flags as u32,
        }
    }

    #[inline]
    fn key_ty(&self) -> u8 {
        (self.flags & KEY_TY_FLAG) as u8
    }

    pub(crate) fn check_pkey_ty(&self, doc: &Document, skipped: &mut bool) -> DbResult<()> {
        let expected = self.key_ty();
        if expected == 0 {
            *skipped = true;
            return Ok(())
        }

        let pkey = &doc.pkey_id().unwrap();
        let actual_ty = pkey.ty_int();

        if expected != actual_ty {
            return Err(DbErr::UnexpectedIdType(expected, actual_ty))
        }

        Ok(())
    }

    pub(crate) fn merge_pkey_ty_to_meta(&mut self, meta_doc: &mut Document, value_doc: &Document) {
        let pkey_ty = value_doc.pkey_id().unwrap().ty_int();
        self.flags |= ((pkey_ty as u32) & KEY_TY_FLAG);
        meta_doc.insert(meta_doc_key::FLAGS.into(), Value::Int(self.flags as i64));
    }

}

pub(crate) mod meta_doc_key {
    pub(crate) static ID: &str       = "_id";
    pub(crate) static ROOT_PID: &str = "root_pid";
    pub(crate) static NAME: &str     = "name";
    pub(crate) static FLAGS: &str    = "flags";
    pub(crate) static INDEXES: &str  = "indexes";

    pub(crate) mod index {
        pub(crate) static NAME: &str = "name";
        pub(crate) static V: &str    = "v";
        pub(crate) static UNIQUE: &str = "unique";
        pub(crate) static ROOT_PID: &str = "root_pid";

    }

}

