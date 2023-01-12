use serde::Serialize;
use bson::{Bson, Document};
use std::borrow::Borrow;
use std::collections::HashMap;
use serde::de::DeserializeOwned;
use crate::{Database, DbErr, DbResult, TransactionType};
use crate::db::db;
use crate::results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

/// A wrapper of collection in struct.
///
/// All CURD methods can be done through this structure.
///
/// It can be used to perform collection-level operations such as CRUD operations.
pub struct Collection<'a, T> {
    db: &'a mut Database,
    name: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T>  Collection<'a, T>
where
    T: Serialize,
{

    pub(super) fn new(db: &'a mut Database, name: &str) -> Collection<'a, T> {
        Collection {
            db,
            name: name.into(),
            _phantom: std::default::Default::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the size of all data in the collection.
    pub fn count_documents(&mut self) -> DbResult<u64> {
        let meta_opt = self.db.get_collection_meta_by_name(&self.name, false)?;
        meta_opt.map_or(Ok(0), |col_meta| {
            self.db.ctx.count(
                col_meta.id,
                col_meta.meta_version
            )
        })
    }

    /// Updates up to one document matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_one(&mut self, query: Document, update: Document) -> DbResult<UpdateResult> {
        let meta_opt = self.db.get_collection_meta_by_name(&self.name, false)?;
        let modified_count: u64 = match meta_opt {
            Some(col_meta) => {
                let size = self.db.ctx.update_one(
                    col_meta.id,
                    col_meta.meta_version,
                    Some(&query),
                    &update
                )?;
                size as u64
            }
            None => 0,
        };
        Ok(UpdateResult {
            modified_count,
        })
    }

    /// Updates all documents matching `query` in the collection.
    /// [documentation](https://www.polodb.org/docs/curd/update) for more information on specifying updates.
    pub fn update_many(&mut self, query: Document, update: Document) -> DbResult<UpdateResult> {
        let meta_opt = self.db.get_collection_meta_by_name(&self.name, false)?;
        let modified_count: u64 = match meta_opt {
            Some(col_meta) => {
                let size = self.db.ctx.update_many(
                    col_meta.id,
                    col_meta.meta_version,
                    Some(&query),
                    &update
                )?;
                size as u64
            }
            None => 0,
        };
        Ok(UpdateResult {
            modified_count,
        })
    }

    /// Inserts `doc` into the collection.
    pub fn insert_one(&mut self, doc: impl Borrow<T>) -> DbResult<InsertOneResult> {
        let mut doc = bson::to_document(doc.borrow())?;
        let col_meta = self.db
            .get_collection_meta_by_name(&self.name, true)?
            .expect("internal: meta must exist");
        let _ = self.db.ctx.insert(col_meta.id, col_meta.meta_version, &mut doc)?;
        let pkey = doc.get("_id").unwrap();
        Ok(InsertOneResult {
            inserted_id: pkey.clone(),
        })
    }

    /// Inserts the data in `docs` into the collection.
    pub fn insert_many(&mut self, docs: impl IntoIterator<Item = impl Borrow<T>>) -> DbResult<InsertManyResult> {
        self.db.start_transaction(Some(TransactionType::Write))?;
        let col_meta = self.db
            .get_collection_meta_by_name(&self.name, true)?
            .expect("internal: meta must exist");
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new();
        let mut counter: usize = 0;

        for item in docs {
            let mut doc = match bson::to_document(item.borrow()) {
                Ok(doc) => doc,
                Err(err) => {
                    self.db.rollback().unwrap();
                    return Err(DbErr::from(err));
                }
            };
            match self.db.ctx.insert(col_meta.id, col_meta.meta_version, &mut doc) {
                Ok(_) => (),
                Err(err) => {
                    self.db.rollback().unwrap();
                    return Err(err);
                }
            }
            let pkey = doc.get("_id").unwrap();
            inserted_ids.insert(counter, pkey.clone());

            counter += 1;
        }

        self.db.commit()?;
        Ok(InsertManyResult {
            inserted_ids,
        })
    }

    /// Deletes up to one document found matching `query`.
    pub fn delete_one(&mut self, query: Document) -> DbResult<DeleteResult> {
        let meta_opt = self.db.get_collection_meta_by_name(&self.name, false)?;
        let deleted_count = match meta_opt {
            Some(col_meta) => {
                let count = self.db.ctx.delete(col_meta.id, col_meta.meta_version,
                                               query, false)?;
                count as u64
            }
            None => 0
        };
        Ok(DeleteResult {
            deleted_count,
        })
    }

    /// When query is `None`, all the data in the collection will be deleted.
    ///
    /// The size of data deleted returns.
    pub fn delete_many(&mut self, query: Document) -> DbResult<DeleteResult> {
        let meta_opt = self.db.get_collection_meta_by_name(&self.name, false)?;
        let deleted_count = match meta_opt {
            Some(col_meta) => {
                let count = if query.len() == 0 {
                    self.db.ctx.delete_all(col_meta.id, col_meta.meta_version)?
                } else {
                    self.db.ctx.delete(col_meta.id, col_meta.meta_version, query, true)?
                };
                count as u64
            }
            None => 0
        };
        Ok(DeleteResult {
            deleted_count,
        })
    }

    /// release in 0.12
    #[allow(dead_code)]
    fn create_index(&mut self, keys: &Document, options: Option<&Document>) -> DbResult<()> {
        let col_meta = self.db
            .get_collection_meta_by_name(&self.name, true)?
            .unwrap();
        self.db.ctx.create_index(col_meta.id, keys, options)
    }

}

impl<'a, T>  Collection<'a, T>
    where
        T: DeserializeOwned,
{
    /// When query document is passed to the function. The result satisfies
    /// the query document.
    pub fn find_many(&mut self, filter: impl Into<Option<Document>>) -> DbResult<Vec<T>> {
        let filter_query = filter.into();
        let meta_opt = self.db.get_collection_meta_by_name(&self.name, false)?;
        match meta_opt {
            Some(col_meta) => {
                let mut handle = self.db.ctx.find(
                    col_meta.id,
                    col_meta.meta_version,
                    filter_query
                )?;

                let mut result: Vec<T> = Vec::new();
                db::consume_handle_to_vec::<T>(&mut handle, &mut result)?;

                Ok(result)

            }
            None => {
                Ok(vec![])
            }
        }
    }

    /// Return the first element in the collection satisfies the query.
    pub fn find_one(&mut self, filter: impl Into<Option<Document>>) -> DbResult<Option<T>> {
        let filter_query = filter.into();
        let meta_opt = self.db.get_collection_meta_by_name(&self.name, false)?;
        let result: Option<T> = if let Some(col_meta) = meta_opt {
            let mut handle = self.db.ctx.find(
                col_meta.id,
                col_meta.meta_version,
                filter_query
            )?;
            handle.step()?;

            if !handle.has_row() {
                handle.commit_and_close_vm()?;
                return Ok(None);
            }

            let result_doc = handle.get().as_document().unwrap().clone();

            handle.commit_and_close_vm()?;

            bson::from_document(result_doc)?
        } else {
            None
        };

        Ok(result)
    }

}
