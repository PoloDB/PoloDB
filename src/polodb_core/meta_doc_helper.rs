use bson::{Document, Bson, doc, bson};
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
    doc: Document,
}

pub(crate) const KEY_TY_FLAG: u32 = 0b11111111;

impl MetaDocEntry {

    pub fn new(id: u32, name: String, root_pid: u32) -> MetaDocEntry {
        let doc = doc! {
            "_id": id,
            "name": name.clone(),
            "root_pid": root_pid as i64,
            "flags": 0,
        };
        MetaDocEntry {
            name,
            doc,
        }
    }

    pub(crate) fn from_doc(doc: Document) -> MetaDocEntry {
        let name = doc.get(meta_doc_key::NAME).unwrap().as_str().unwrap();
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
        self.doc.get(meta_doc_key::ROOT_PID).unwrap().as_i64().unwrap() as u32
    }

    pub(crate) fn set_root_pid(&mut self, new_root_pid: u32) {
        self.doc.insert::<String, Bson>(meta_doc_key::ROOT_PID.into(), Bson::Int64(new_root_pid as i64));
    }

    pub(crate) fn flags(&self) -> u32 {
        self.doc.get(meta_doc_key::FLAGS).unwrap().as_i32().unwrap() as u32
    }

    pub(crate) fn set_flags(&mut self, flags: u32) {
        self.doc.insert::<String, Bson>(meta_doc_key::FLAGS.into(), bson!(flags as i32));
    }

    #[inline]
    fn key_ty(&self) -> u8 {
        (self.flags() & KEY_TY_FLAG) as u8
    }

    pub(crate) fn check_pkey_ty(&self, primary_key: &Bson, skipped: &mut bool) -> DbResult<()> {
        let expected = self.key_ty();
        if expected == 0 {
            *skipped = true;
            return Ok(())
        }

        let actual_ty = primary_key.element_type() as u8;

        if expected != actual_ty {
            return Err(DbErr::UnexpectedIdType(expected, actual_ty))
        }

        Ok(())
    }

    pub(crate) fn merge_pkey_ty_to_meta(&mut self, value_doc: &Document) {
        let pkey_ty = value_doc.get("_id").unwrap().element_type();
        self.set_flags(self.flags() | ((pkey_ty as u32) & KEY_TY_FLAG));
    }

    #[inline]
    pub(crate) fn doc_ref(&self) -> &Document {
        &self.doc
    }

    pub(crate) fn set_indexes(&mut self, indexes: Document) {
        self.doc.insert::<String, Bson>(meta_doc_key::INDEXES.into(), Bson::from(indexes));
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

