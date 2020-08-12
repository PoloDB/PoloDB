// root_btree schema
// {
//   _id: ObjectId,
//   name: String,
//   root_pid: Int,
//   flags: Int,
// }
//
// flags indicates:
// key_ty: 1byte
// ...
//
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::LinkedList;
use std::ops::DerefMut;
use super::error::DbErr;
use super::page::{ header_page_utils, PageHandler };
use crate::bson::object_id::ObjectIdMaker;
use crate::overflow_data::{ OverflowDataWrapper, OverflowDataTicket };
use crate::bson::{ObjectId, Document, value};
use crate::btree::BTreePageWrapper;

static DB_INIT_BLOCK_COUNT: u32 = 16;

// #[derive(Clone)]
pub struct Database {
    ctx: Box<DbContext>,
}

pub type DbResult<T> = Result<T, DbErr>;

pub(crate) struct DbContext {
    page_handler :        Rc<RefCell<PageHandler>>,
    pending_block_offset: u32,
    overflow_data_pages:  LinkedList<u32>,

    pub obj_id_maker: ObjectIdMaker,

}

impl DbContext {

    fn new(path: &str) -> DbResult<DbContext> {
        let page_size = 4096;

        let page_handler = PageHandler::new(path, page_size)?;

        let obj_id_maker = ObjectIdMaker::new();

        let ctx = DbContext {
            page_handler: Rc::new(RefCell::new(page_handler)),

            pending_block_offset: 0,
            overflow_data_pages: LinkedList::new(),

            // first_page,
            obj_id_maker,
        };
        Ok(ctx)
    }

    fn alloc_overflow_ticker(&mut self, size: u32) -> DbResult<OverflowDataTicket> {
        let mut page_handler = self.page_handler.as_ref().borrow_mut();
        let page_id = page_handler.alloc_page_id()?;

        self.overflow_data_pages.push_back(page_id);

        let raw_page = page_handler.pipeline_read_page(page_id)?;

        let mut overflow = OverflowDataWrapper::from_raw_page(self.page_handler.clone(), raw_page)?;

        let ticket = overflow.alloc(size)?;

        Ok(OverflowDataTicket {
            items: vec![ ticket ],
        })
    }

    #[inline]
    fn get_meta_page_id(&mut self) -> DbResult<u32> {
        let mut page_handler = self.page_handler.as_ref().borrow_mut();
        let head_page = page_handler.pipeline_read_page(0)?;
        Ok(header_page_utils::get_meta_page_id(&head_page))
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<ObjectId> {
        let oid = self.obj_id_maker.mk_object_id();
        let mut doc = Document::new_without_id();
        doc.insert("_id".into(), value::Value::ObjectId(oid.clone()));

        doc.insert("name".into(), value::Value::String(name.into()));

        let root_pid = {
            let mut page_handler = self.page_handler.as_ref().borrow_mut();
            page_handler.alloc_page_id()?
        };
        doc.insert("root_pid".into(), value::Value::Int(root_pid as i64));

        doc.insert("flags".into(), value::Value::Int(0));

        let meta_page_id: u32 = self.get_meta_page_id()?;

        let mut page_handler = self.page_handler.borrow_mut();
        let mut btree_wrapper = BTreePageWrapper::new(page_handler.deref_mut(), meta_page_id);

        let backward = btree_wrapper.insert_item(Rc::new(doc), false)?;

        match backward {
            Some(backward_item) => {
                let mut page_handler = self.page_handler.as_ref().borrow_mut();
                let new_root_id = page_handler.alloc_page_id()?;

                let raw_page = backward_item.write_to_page(new_root_id, meta_page_id, page_handler.page_size)?;

                // update head page
                {
                    let mut head_page = page_handler.pipeline_read_page(0)?;
                    header_page_utils::set_meta_page_id(&mut head_page, new_root_id);
                    page_handler.pipeline_write_page(&head_page)?;
                }

                page_handler.pipeline_write_page(&raw_page)?;

                Ok(oid)
            }

            None => Ok(oid)
        }
    }

    pub fn query_all_meta(&mut self) -> DbResult<Vec<Rc<Document>>> {
        let meta_page_id = self.get_meta_page_id()?;
        let mut page_handler = self.page_handler.as_ref().borrow_mut();
        let mut btree_wrapper = BTreePageWrapper::new(page_handler.deref_mut(), meta_page_id);

        btree_wrapper.query_all_data()
    }

}

impl Drop for DbContext {

    fn drop(&mut self) {
        let _ = self.page_handler.as_ref().borrow_mut().checkpoint_journal();  // ignored
    }

}

impl Database {

    pub fn open(path: &str) -> DbResult<Database>  {
        let ctx = DbContext::new(path)?;
        let rc_ctx = Box::new(ctx);

        Ok(Database {
            ctx: rc_ctx,
        })
    }

    pub fn create_collection(&mut self, name: &str) -> DbResult<ObjectId> {
        self.ctx.create_collection(name)
    }

    pub fn get_version(&self) -> String {
        const VERSION: &'static str = env!("CARGO_PKG_VERSION");
        return VERSION.into();
    }

    pub fn insert(&mut self, col_name: &str, doc: Rc<Document>) -> DbResult<()> {
        let meta = self.query_all_meta()?;

        for item in &meta {
            match item.map.get(col_name) {
                Some(value::Value::String(name)) => {
                    if name == col_name {  // found
                        let page_id = item.map.get("page_id").unwrap();
                        match page_id {
                            value::Value::Int(_page_id) => {
                                // TODO: insert iterm
                                return Ok(())
                            }
                            _ => panic!("page id is not int type")
                        }
                    }
                }
                _ => ()
            }
        }

        Err(DbErr::CollectionNotFound(col_name.into()))
    }

    pub(crate) fn query_all_meta(&mut self) -> DbResult<Vec<Rc<Document>>> {
        self.ctx.query_all_meta()
    }

}

#[cfg(test)]
mod tests {
    use crate::Database;

    #[test]
    fn test_create_collection() {
        let _ = std::fs::remove_file("/tmp/test.db");
        let _ = std::fs::remove_file("/tmp/test.db.journal");

        let mut db = Database::open("/tmp/test.db").unwrap();
        let result = db.create_collection("test").unwrap();
        println!("object:id {}", result.to_string());

        let meta = db.query_all_meta().unwrap();

        for (index, doc) in meta.iter().enumerate() {
            println!("index: {}, object: {}", index, doc)
        }
    }

}
