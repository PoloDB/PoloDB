use bson::Document;
use serde::{Serialize, Deserialize};
use crate::TransactionType;

#[derive(Serialize, Deserialize)]
pub struct FindCommand {
    pub ns: String,
    pub multi: bool,
    pub filter: Option<Document>,
}

#[derive(Serialize, Deserialize)]
pub struct InsertCommand {
    pub ns: String,
    pub documents: Vec<Document>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateCommand {
    pub ns: String,
    pub filter: Document,
    pub update: Document,
    pub multi: bool,
}

#[derive(Serialize, Deserialize)]
pub struct DeleteCommand {
    pub ns: String,
    pub filter: Document,
    pub multi: bool,
}

#[derive(Serialize, Deserialize)]
pub struct CreateCollectionCommand {
    pub ns: String,
}

#[derive(Serialize, Deserialize)]
pub struct DropCollectionCommand {
    pub ns: String,
}

#[derive(Serialize, Deserialize)]
pub struct CountDocumentsCommand {
    pub ns: String,
}

#[derive(Serialize, Deserialize)]
pub struct StartTransactionCommand {
    pub ty: Option<TransactionType>,
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
    Commit,
    Rollback,
    SafelyQuit,
}
