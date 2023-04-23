/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::sync::{Arc, Mutex};
use bson::{Bson, Document};
use bson::oid::ObjectId;
use hashbrown::HashMap;
use crate::{ClientSession, Database, Error, ErrorKind, Result};
use crate::commands::{CommandMessage, CommitTransactionCommand, CountDocumentsCommand, CreateCollectionCommand, DeleteCommand, DropCollectionCommand, FindCommand, InsertCommand, AbortTransactionCommand, StartTransactionCommand, UpdateCommand, DropSessionCommand};
use crate::results::CountDocumentsResult;

#[derive(Clone)]
pub struct HandleRequestResult {
    pub is_quit: bool,
    pub value: Bson,
}

pub struct DatabaseServer {
    db: Database,
    session_map: Mutex<HashMap<ObjectId, Arc<Mutex<ClientSession>>>>,
}

impl DatabaseServer {

    pub fn new(db: Database) -> DatabaseServer {
        DatabaseServer {
            db,
            session_map: Mutex::new(HashMap::new()),
        }
    }

    pub fn handle_request_doc(&self, value: Bson) -> Result<HandleRequestResult> {
        let command_message = bson::from_bson::<CommandMessage>(value)?;
        let is_quit = if let CommandMessage::SafelyQuit = command_message {
            true
        } else {
            false
        };

        let result_value: Bson = match command_message {
            CommandMessage::Find(find) => {
                self.handle_find_operation(find)?
            }
            CommandMessage::Insert(insert) => {
                self.handle_insert_operation(insert)?
            }
            CommandMessage::Update(update) => {
                self.handle_update_operation(update)?
            }
            CommandMessage::Delete(delete) => {
                self.handle_delete_operation(delete)?
            }
            CommandMessage::CreateCollection(create_collection) => {
                self.handle_create_collection(create_collection)?
            }
            CommandMessage::DropCollection(drop_collection) => {
                self.handle_drop_collection(drop_collection)?
            }
            CommandMessage::StartTransaction(start_transaction) => {
                self.handle_start_transaction(start_transaction)?
            }
            CommandMessage::CommitTransaction(commit) => {
                self.handle_commit(commit)?
            }
            CommandMessage::AbortTransaction(rollback) => {
                self.handle_rollback(rollback)?
            }
            CommandMessage::SafelyQuit => {
                Bson::Null
            }
            CommandMessage::CountDocuments(count_documents) => {
                self.handle_count_operation(count_documents)?
            }
            CommandMessage::StartSession => {
                self.handle_start_session()?
            }
            CommandMessage::DropSession(drop_session) => {
                self.handle_drop_session(drop_session)?
            }
        };


        Ok(HandleRequestResult {
            is_quit,
            value: result_value,
        })
    }

    fn get_session_by_session_id(&self, sid: Option<&ObjectId>) -> Result<Arc<Mutex<ClientSession>>> {
        match sid {
            Some(sid) => {
                let session_map = self.session_map.lock()?;
                Ok(session_map.get(sid).unwrap().clone())
            }
            None => {
                let session = self.db.start_session()?;
                Ok(Arc::new(Mutex::new(session)))
            }
        }
    }

    fn handle_find_operation(&self, find: FindCommand) -> Result<Bson> {
        let col_name = find.ns.as_str();
        let session_id = find.options
            .as_ref()
            .map(|o| o.session_id.as_ref())
            .flatten();
        let session_ref = self.get_session_by_session_id(session_id)?;
        let mut session = session_ref.lock()?;
        let collection = self.db.collection::<Document>(col_name);
        let mut result = collection.find_with_session(find.filter, &mut session)?;

        let mut value_arr = bson::Array::new();

        let mut counter : usize = 0;
        while result.advance(&mut session)? {
            if !find.multi && counter > 0 {
                break;
            }

            value_arr.push(result.get().clone());

            counter += 1;
        }

        let result_value = Bson::Array(value_arr);
        Ok(result_value)
    }

    fn handle_insert_operation(&self, insert: InsertCommand) -> Result<Bson> {
        let col_name = insert.ns.as_str();
        let session_id = insert.options
            .as_ref()
            .map(|o| o.session_id.as_ref())
            .flatten();
        let session_ref = self.get_session_by_session_id(session_id)?;
        let mut session = session_ref.lock()?;
        let collection = self.db.collection::<Document>(col_name);
        let insert_result = collection.insert_many_with_session(insert.documents, &mut session)?;
        let bson_val = bson::to_bson(&insert_result)?;
        Ok(bson_val)
    }

    fn handle_update_operation(&self, update: UpdateCommand) -> Result<Bson> {
        let col_name: &str = &update.ns;

        let session_id = update.options
            .as_ref()
            .map(|o| o.session_id.as_ref())
            .flatten();

        let session_ref = self.get_session_by_session_id(session_id)?;
        let mut session = session_ref.lock()?;
        let collection = self.db.collection::<Document>(col_name);

        let result = if update.multi {
            collection.update_many_with_session(update.filter, update.update, &mut session)?
        } else {
            collection.update_one_with_session(update.filter, update.update, &mut session)?
        };

        let bson_val = bson::to_bson(&result)?;
        Ok(bson_val)
    }

    fn handle_delete_operation(&self, delete: DeleteCommand) -> Result<Bson> {
        let col_name: &str = &delete.ns;

        let session_id = delete.options
            .as_ref()
            .map(|o| o.session_id.as_ref())
            .flatten();

        let session_ref = self.get_session_by_session_id(session_id)?;
        let mut session = session_ref.lock()?;
        let collection = self.db.collection::<Document>(col_name);

        let result = if delete.multi {
            collection.delete_many_with_session(delete.filter, &mut session)?
        } else {
            collection.delete_one_with_session(delete.filter, &mut session)?
        };

        let bson_val = bson::to_bson(&result)?;
        Ok(bson_val)
    }

    fn handle_create_collection(&self, create_collection: CreateCollectionCommand) -> Result<Bson> {
        let session_id = create_collection.options
            .as_ref()
            .map(|o| o.session_id.as_ref())
            .flatten();

        let session_ref = self.get_session_by_session_id(session_id)?;
        let mut session = session_ref.lock()?;

        let ret = match self.db.create_collection_with_session(
            &create_collection.ns,
            &mut session,
        ) {
            Ok(_) => true,
            Err(Error(ErrorKind::CollectionAlreadyExits(_), _)) => false,
            Err(err) => return Err(err),
        };

        Ok(Bson::Boolean(ret))
    }

    fn handle_drop_collection(&self, drop_command: DropCollectionCommand) -> Result<Bson> {
        let col_name = &drop_command.ns;
        let session_id = drop_command.options
            .as_ref()
            .map(|o| o.session_id.as_ref())
            .flatten();
        let session_ref = self.get_session_by_session_id(session_id)?;
        let mut session = session_ref.lock()?;

        let collection = self.db.collection::<Document>(col_name);
        collection.drop_with_session(&mut session)?;

        Ok(Bson::Null)
    }

    fn handle_count_operation(&self, count_documents: CountDocumentsCommand) -> Result<Bson> {
        let session_id = count_documents.options
            .as_ref()
            .map(|o| o.session_id.as_ref())
            .flatten();
        let session_ref = self.get_session_by_session_id(session_id)?;
        let mut session = session_ref.lock()?;

        let collection = self.db.collection::<Document>(&count_documents.ns);
        let count = collection.count_documents_with_session(&mut session)?;

        let bson_val = bson::to_bson(&CountDocumentsResult {
            count
        })?;
        Ok(bson_val)
    }

    fn handle_start_transaction(&self, start_transaction_command: StartTransactionCommand) -> Result<Bson> {
        let session_ref = self.get_session_by_session_id(Some(&start_transaction_command.session_id))?;
        let mut session = session_ref.lock()?;
        session.start_transaction(start_transaction_command.ty)?;
        Ok(Bson::Null)
    }

    fn handle_commit(&self, commit_command: CommitTransactionCommand) -> Result<Bson> {
        let session_ref = self.get_session_by_session_id(Some(&commit_command.session_id))?;
        let mut session = session_ref.lock()?;
        session.commit_transaction()?;
        Ok(Bson::Null)
    }

    fn handle_rollback(&self, rollback_command: AbortTransactionCommand) -> Result<Bson> {
        let session_ref = self.get_session_by_session_id(Some(&rollback_command.session_id))?;
        let mut session = session_ref.lock()?;
        session.abort_transaction()?;
        Ok(Bson::Null)
    }

    fn handle_start_session(&self) -> Result<Bson> {
        let mut session_map = self.session_map.lock()?;
        let sid = ObjectId::new();
        let session = self.db.start_session()?;
        session_map.insert(sid.clone(), Arc::new(Mutex::new(session)));
        Ok(Bson::ObjectId(sid))
    }

    fn handle_drop_session(&self, drop_session_command: DropSessionCommand) -> Result<Bson> {
        let mut session_map = self.session_map.lock()?;
        session_map.remove(&drop_session_command.session_id);
        Ok(Bson::Null)
    }

}
