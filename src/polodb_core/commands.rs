use bson::Document;
use serde::{Serialize, Deserialize};
use crate::TransactionType;

#[derive(Serialize, Deserialize)]
pub struct FindCommand {
    ns: String,
    multi: bool,
    filter: Option<Document>,
}

#[derive(Serialize, Deserialize)]
pub struct InsertCommand {
    ns: String,
    documents: Vec<Document>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateCommand {
    ns: String,
    filter: Document,
    update: Document,
}

#[derive(Serialize, Deserialize)]
pub struct DeleteCommand {
    ns: String,
    filter: Document,
    multi: bool,
}

#[derive(Serialize, Deserialize)]
pub struct CreateCollectionCommand {
    ns: String,
}

#[derive(Serialize, Deserialize)]
pub struct DropCollectionCommand {
    ns: String,
}

#[derive(Serialize, Deserialize)]
pub struct CountDocumentsCommand {
    ns: String,
}

#[derive(Serialize, Deserialize)]
pub struct StartTransactionCommand {
    ty: Option<TransactionType>,
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
