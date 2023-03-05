/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use bson::Document;
use bson::oid::ObjectId;
use serde::{Serialize, Deserialize};
use crate::TransactionType;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindCommandOptions {
    pub session_id: Option<ObjectId>,
}

#[derive(Serialize, Deserialize)]
pub struct FindCommand {
    pub ns: String,
    pub multi: bool,
    pub filter: Option<Document>,
    pub options: Option<FindCommandOptions>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertCommandOptions {
    pub session_id: Option<ObjectId>,
}

#[derive(Serialize, Deserialize)]
pub struct InsertCommand {
    pub ns: String,
    pub documents: Vec<Document>,
    pub options: Option<InsertCommandOptions>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateCommandOptions {
    pub session_id: Option<ObjectId>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateCommand {
    pub ns: String,
    pub filter: Document,
    pub update: Document,
    pub multi: bool,
    pub options: Option<UpdateCommandOptions>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteCommandOptions {
    pub session_id: Option<ObjectId>,
}

#[derive(Serialize, Deserialize)]
pub struct DeleteCommand {
    pub ns: String,
    pub filter: Document,
    pub multi: bool,
    pub options: Option<DeleteCommandOptions>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateCollectionCommandOptions {
    pub session_id: Option<ObjectId>,
}

#[derive(Serialize, Deserialize)]
pub struct CreateCollectionCommand {
    pub ns: String,
    pub options: Option<CreateCollectionCommandOptions>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DropCollectionCommandOptions {
    pub session_id: Option<ObjectId>,
}

#[derive(Serialize, Deserialize)]
pub struct DropCollectionCommand {
    pub ns: String,
    pub options: Option<DropCollectionCommandOptions>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountDocumentsCommandOptions {
    pub session_id: Option<ObjectId>,
}

#[derive(Serialize, Deserialize)]
pub struct CountDocumentsCommand {
    pub ns: String,
    pub options: Option<CountDocumentsCommandOptions>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartTransactionCommand {
    pub ty: Option<TransactionType>,
    pub session_id: ObjectId,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitCommand {
    pub session_id: ObjectId,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollbackCommand {
    pub session_id: ObjectId,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "command")]
pub enum CommandMessage {
    Find(FindCommand),
    Insert(InsertCommand),
    Update(UpdateCommand),
    Delete(DeleteCommand),
    CreateCollection(CreateCollectionCommand),
    DropCollection(DropCollectionCommand),
    CountDocuments(CountDocumentsCommand),
    StartTransaction(StartTransactionCommand),
    Commit(CommitCommand),
    Rollback(RollbackCommand),
    SafelyQuit,
}
