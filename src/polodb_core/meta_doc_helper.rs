use polodb_bson::{Document, Value, mk_document};
use std::rc::Rc;
use crate::DbResult;
use crate::error::DbErr;

/// root_btree schema
/// {
///   _id: String,
///   name: String,
///   root_pid: Int,
///   flags: Int,
/// }
///
/// flags indicates:
/// key_ty: 1byte
/// ...
///
pub(crate) struct MetaDocEntry {
    name: String,
    doc: Rc<Document>,
}

pub(crate) const KEY_TY_FLAG: u32 = 0b11111111;

impl MetaDocEntry {

    pub fn new(id: u32, name: String, root_pid: u32) -> MetaDocEntry {
        let doc = mk_document! {
            "_id": id,
            "name": name.clone(),
            "root_pid": root_pid,
            "flags": 0,
        };
        MetaDocEntry {
            name,
            doc: Rc::new(doc),
        }
    }

    pub(crate) fn from_doc(doc: Rc<Document>) -> MetaDocEntry {
        let name = doc.get(meta_doc_key::NAME).unwrap().unwrap_string();
        MetaDocEntry {
            name: name.into(),
            doc,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    pub(crate) fn root_pid(&self) -> u32 {
        self.doc.get(meta_doc_key::ROOT_PID).unwrap().unwrap_int() as u32
    }

    pub(crate) fn set_root_pid(&mut self, new_root_pid: u32) {
        let doc_mut = Rc::get_mut(&mut self.doc).unwrap();
        doc_mut.insert(meta_doc_key::ROOT_PID.into(), Value::from(new_root_pid));
    }

    pub(crate) fn flags(&self) -> u32 {
        self.doc.get(meta_doc_key::FLAGS).unwrap().unwrap_int() as u32
    }

    pub(crate) fn set_flags(&mut self, flags: u32) {
        let doc_mut = Rc::get_mut(&mut self.doc).unwrap();
        doc_mut.insert(meta_doc_key::FLAGS.into(), Value::from(flags));
    }

    #[inline]
    fn key_ty(&self) -> u8 {
        (self.flags() & KEY_TY_FLAG) as u8
    }

    pub(crate) fn check_pkey_ty(&self, primary_key: &Value, skipped: &mut bool) -> DbResult<()> {
        let expected = self.key_ty();
        if expected == 0 {
            *skipped = true;
            return Ok(())
        }

        let actual_ty = primary_key.ty_int();

        if expected != actual_ty {
            return Err(DbErr::UnexpectedIdType(expected, actual_ty))
        }

        Ok(())
    }

    pub(crate) fn merge_pkey_ty_to_meta(&mut self, value_doc: &Document) {
        let pkey_ty = value_doc.pkey_id().unwrap().ty_int();
        self.set_flags(self.flags() | ((pkey_ty as u32) & KEY_TY_FLAG));
    }

    #[inline]
    pub(crate) fn doc_ref(&self) -> &Document {
        self.doc.as_ref()
    }

    pub(crate) fn set_indexes(&mut self, indexes: Document) {
        let doc_mut = Rc::get_mut(&mut self.doc).unwrap();
        doc_mut.insert(meta_doc_key::INDEXES.into(), Value::from(indexes));
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

