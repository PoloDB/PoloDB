// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This is the main entry point for the PoloDB server.
//! This file includes a command-line interface for starting the server.
//!
//! You can start the server by running `cargo run -- serve --path /path/to/db`.
//!
//! The server will listen on `localhost:27017` by default.
//! You can also specify the host and port by passing `--host` and `--port` arguments.
//! For example: `cargo run -- serve --host 0.0.0.0 --port 8080 --path /path/to/db`.
//!
//! # Connect
//!
//! You can connect to the server using the `mongo` shell.
//! And the official rust driver is also supported.
//! You can check the [official driver](https://crates.io/crates/mongodb) for more information.
//!

mod wire;
mod bson_util;
mod compression;
pub(crate) mod sync_read_ext;
pub(crate) mod checked;
mod reply;
mod handlers;
mod app_context;
mod utils;
mod session_context;

use std::net::SocketAddr;
use polodb_core::Database;
use bson::{rawdoc, Document, RawBsonRef};
use clap::{Arg, Command as App};
use anyhow::{Result, anyhow};
use tokio::io::{AsyncRead, AsyncWrite};
use log::{info, warn, error, debug};
use tokio::select;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use reply::Reply;
use crate::app_context::AppContext;
use crate::handlers::{make_handlers, HandleContext};
use crate::utils::uuid_from_bson;

#[tokio::main]
async fn main() {
    env_logger::init();
    let version = Database::get_version();
    let app = App::new("PoloDB")
        .version(version)
        .about("Command line tool for PoloDB")
        .author("Vincent Chan <okcdz@diverse.space>")
        .subcommand(App::new("serve")
            .about("attach the database, start the tcp server")
            .arg(
                Arg::new("host")
                    .long("host")
                    .help("the host address")
                    .default_value("localhost")
                    .num_args(1)
            )
            .arg(
                Arg::new("port")
                    .long("port")
                    .help("the port number")
                    .default_value("27017")
                    .num_args(1)
            )
            .arg(
                Arg::new("path")
                    .short('p')
                    .long("path")
                    .value_name("PATH")
                    .num_args(0..=1)
            )
            .arg(Arg::new("memory"))
            .arg(
                Arg::new("log")
                    .help("print log")
                    .long("log")
                    .short('l')
            )
        )
        .arg(
            Arg::new("log")
                .help("print log")
                .long("log")
                .short('l')
        );

    let matches = app.get_matches();

    if let Some(sub) = matches.subcommand_matches("serve") {
        let should_log = sub.contains_id("log");
        Database::set_log(should_log);

        let host = sub.get_one::<String>("host").unwrap();
        let port = sub.get_one::<String>("port").unwrap();
        let path = sub.get_one::<String>("path");
        if let Some(path) = path {
            let socket = format!("{}:{}", host, port);
            let token = CancellationToken::new();
            let result = start_socket_server(path.clone(), socket.to_string(), token).await;
            match result {
                Ok((addr, fut)) => {
                    info!("listening on {}", addr);
                    fut.await.unwrap();
                }
                Err(e) => {
                    error!("error: {:?}", e);
                }
            }
        } else {
            eprintln!("you should pass --path ");
        }
        return;
    }

}

pub(crate) async fn start_socket_server(path: String, socket: String, token: CancellationToken) -> Result<(SocketAddr, JoinHandle<()>)> {
    let db = Database::open_path(&path)?;

    let ctx = AppContext::new(db);

    ctx.register_handlers(make_handlers());

    let listener = tokio::net::TcpListener::bind(&socket).await?;
    let addr = listener.local_addr()?;

    let fut = tokio::spawn(async move {
        loop {
            select! {
                _ = token.cancelled() => {
                    info!("server stopped");
                    return
                }

                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            let ctx = ctx.clone();
                            tokio::spawn(async move {
                                let conn_id = ctx.next_conn_id();
                                info!("new connection: {} from {}", conn_id, addr);
                                let result = handle_stream(ctx.clone(), conn_id, stream).await;
                                if let Err(e) = result {
                                    // if is unexpected end of file, ignore if
                                    if e.to_string().contains("unexpected end of file") {
                                        return
                                    }
                                    error!("handle stream error: {:?}", e);
                                }
                                info!("connection closed: {}", conn_id);
                            });
                        }
                        Err(err) => {
                            warn!("accept error: {:?}", err);
                            return
                        }
                    }
                }
            }
        }
    });

    Ok((addr, fut))
}

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

async fn handle_stream<W: AsyncWrite + AsyncRead + Unpin + Send>(ctx: AppContext, conn_id: u64, mut stream: W) -> Result<()> {
    loop {
        let ctx = ctx.clone();
        let message = wire::Message::read_from(&mut stream, Some(MAX_MESSAGE_SIZE as i32)).await?;
        debug!("received: {:?}", message);

        handle_message(ctx, conn_id, &mut stream, message).await?;
    }
}

async fn handle_message<W: AsyncWrite + Unpin>(ctx: AppContext, conn_id: u64, stream: &mut W, message: wire::Message) -> Result<()> {
    let handler = ctx.get_handlers(&message.document_payload)?;
    if let Some(handler) = handler {
        let start_transaction = utils::truly_value_for_bson_ref(message.document_payload.get("startTransaction")?, false);
        let session = if start_transaction {
            let lsid = message.document_payload.get_document("lsid")?;
            let lsid_doc = bson::from_slice::<Document>(lsid.as_bytes())?;
            let id = uuid_from_bson(
                lsid_doc.get("id").ok_or(anyhow!("lsid missing id field"))?,
            ).ok_or(anyhow!("lsid missing id field"))?;
            info!("=== start transaction: {:?}, id: {:?}", lsid_doc, id);
            let txn = ctx.db().start_transaction()?;
            Some(ctx.create_session(id, txn))
        } else {
            let lsid_opt = message.document_payload.get("lsid")?;
            match lsid_opt {
                Some(RawBsonRef::Document(lsid_doc)) => {
                    let lsid_doc = bson::from_slice::<Document>(lsid_doc.as_bytes())?;
                    let id = uuid_from_bson(
                        lsid_doc.get("id").ok_or(anyhow!("lsid missing id field"))?,
                    ).ok_or(anyhow!("lsid missing id field"))?;
                    ctx.get_session(&id)
                }
                _ => None
            }
        };
        let auto_commit = utils::truly_value_for_bson_ref(message.document_payload.get("autocommit")?, true);

        let ctx = HandleContext {
            app_context: ctx.clone(),
            conn_id,
            message: &message,
            session,
            auto_commit,
        };
        let reply_result = handler.handle(&ctx).await;
        match reply_result {
            Ok(reply) => {
                reply.write_to(stream).await?;
            }
            Err(e) => {
                log::error!("handler error: {:?}", e);
                let doc = rawdoc! {
                    "ok": 0,
                    "errmsg": e.to_string(),
                    "code": 1,
                };
                let reply = Reply::new(message.request_id.unwrap(), doc);
                reply.write_to(stream).await?;
            }
        }
    } else {
        log::error!("no handler found for message: {:?}", message);
        let doc = rawdoc! {
            "ok": 0,
            "errmsg": "no handler found",
        };
        let reply = Reply::new(message.request_id.unwrap(), doc);
        reply.write_to(stream).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use tokio_util::sync::CancellationToken;
    use anyhow::Result;
    use crate::start_socket_server;

    #[async_trait]
    trait Runner {
        async fn run(&self, client: mongodb::Client) -> Result<()>;
    }

    async fn open_server_with_test(path: &std::path::Path, callback: Box<dyn Runner>) -> Result<()> {
        use mongodb::Client;

        std::env::set_var("RUST_LOG", "polodb=debug,tokio=info, mongodb=debug");
        let _ = env_logger::try_init();

        let token = CancellationToken::new();

        let (addr, handle) = start_socket_server(
            path.to_str().unwrap().to_string(),
            "localhost:0".to_string(),
            token.clone(),
        ).await.unwrap();
        assert!(addr.port() > 0);

        let uri = format!("mongodb://localhost:{}", addr.port());
        let client = Client::with_uri_str(uri).await.unwrap();
        callback.run(client).await?;

        token.cancel();
        handle.await?;
        Ok(())
    }

    fn mk_db_path(db_name: &str) -> std::path::PathBuf {
        let mut db_path = std::env::temp_dir();
        let db_filename = String::from(db_name) + "-db-server";
        db_path.push(db_filename);
        db_path
    }

    #[tokio::test]
    async fn test_server() {
        use mongodb::{
            bson::{Document, doc},
            Collection
        };

        struct TestRunner;
        #[async_trait::async_trait]
        impl Runner for TestRunner {
            async fn run(&self, client: mongodb::Client) -> Result<()> {
                let database = client.database("sample_mflix");
                let my_coll: Collection<Document> = database.collection("movies");
                my_coll.insert_one(doc! { "x": 1 }).await.unwrap();
                Ok(())
            }
        }

        let db_path = mk_db_path("test-server");
        open_server_with_test(db_path.as_path(), Box::new(TestRunner)).await.unwrap();
    }

    #[tokio::test]
    async fn test_find() {
        use mongodb::{
            bson::{Document, doc},
            Collection
        };
        use futures::TryStreamExt;

        let db_path = mk_db_path("test-find");

        struct TestRunner;

        #[async_trait::async_trait]
        impl Runner for TestRunner {
            async fn run(&self, client: mongodb::Client) -> Result<()> {
                let mut docs: Vec<Document> = Vec::with_capacity(1000);
                for i in 0..1000 {
                    docs.push(doc! {
                        "_id": i,
                        "x": i.to_string(),
                    });
                }

                let database = client.database("sample_mflix");
                let my_coll: Collection<Document> = database.collection("movies");
                my_coll.insert_many(docs).await.unwrap();

                let mut cursor = my_coll.find(doc! {}).await.unwrap();

                let mut all = Vec::<Document>::new();

                while let Some(doc) = cursor.try_next().await.unwrap() {
                    all.push(doc);
                }

                assert_eq!(1000, all.len());

                // test limit
                let cursor = my_coll.find(doc! {}).limit(10).await.unwrap();
                let all = cursor.try_collect::<Vec<Document>>().await.unwrap();
                assert_eq!(10, all.len());

                // test offset
                let cursor = my_coll.find(doc! {}).skip(10).await.unwrap();
                let all = cursor.try_collect::<Vec<Document>>().await.unwrap();
                assert_eq!(990, all.len());

                Ok(())
            }
        }

        open_server_with_test(db_path.as_path(), Box::new(TestRunner)).await.unwrap();
    }

    #[tokio::test]
    async fn test_cursor_drop() {
        use mongodb::{
            bson::{Document, doc},
            Client,
            Collection
        };

        use futures::TryStreamExt;
        std::env::set_var("RUST_LOG", "polodb=debug,tokio=info, mongodb=debug");
        let _ = env_logger::try_init();

        let db_path = mk_db_path("test-cursor-drop");
        let token = CancellationToken::new();

        let (addr, handle) = start_socket_server(
            db_path.to_str().unwrap().to_string(),
            "localhost:0".to_string(),
            token.clone(),
        ).await.unwrap();
        assert!(addr.port() > 0);

        let uri = format!("mongodb://localhost:{}", addr.port());
        let client = Client::with_uri_str(uri).await.unwrap();

        {
            let mut docs: Vec<Document> = Vec::with_capacity(1000);
            for i in 0..1000 {
                docs.push(doc! {
                    "_id": i,
                    "x": i.to_string(),
                });
            }

            let database = client.database("sample_mflix");
            let my_coll: Collection<Document> = database.collection("movies");
            my_coll.insert_many(docs).await.unwrap();

            let mut cursor = my_coll.find(doc! {}).await.unwrap();
            let _ = cursor.try_next().await.unwrap();

        }
        // sleep
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        // wait for killCursors command to be sent
        // no crash is success

        token.cancel();
        handle.await.unwrap()
    }

    #[tokio::test]
    async fn test_update() {
        use mongodb::{
            bson::{Document, doc},
            Collection
        };


        let db_path = mk_db_path("test-update");

        struct TestRunner;
        #[async_trait::async_trait]
        impl Runner for TestRunner {

            async fn run(&self, client: mongodb::Client) -> Result<()> {
                let mut docs: Vec<Document> = Vec::with_capacity(100);
                for i in 0..100 {
                    docs.push(doc! {
                        "_id": i,
                        "x": i,
                    });
                }

                let database = client.database("sample_mflix");
                let my_coll: Collection<Document> = database.collection("movies");
                let insert_result = my_coll.insert_many(docs).await.unwrap();
                assert_eq!(insert_result.inserted_ids.len(), 100);

                let result = my_coll.update_many(doc! {
                    "x": {
                        "$lt": 50
                    }
                }, doc! { "$set": { "x": "updated" } }).await.unwrap();

                assert_eq!(50, result.modified_count);
                Ok(())
            }
        }

        open_server_with_test(db_path.as_path(), Box::new(TestRunner)).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete() {
        use mongodb::{
            bson::{Document, doc},
            Collection
        };

        struct TestRunner;

        #[async_trait::async_trait]
        impl Runner for TestRunner {

            async fn run(&self, client: mongodb::Client) -> Result<()> {
                let mut docs: Vec<Document> = Vec::with_capacity(100);
                for i in 0..100 {
                    docs.push(doc! {
                        "_id": i,
                        "x": i,
                    });
                }

                let database = client.database("sample_mflix");
                let my_coll: Collection<Document> = database.collection("movies");
                let insert_result = my_coll.insert_many(docs).await.unwrap();
                assert_eq!(insert_result.inserted_ids.len(), 100);

                let result = my_coll.delete_many(doc! {
                    "x": {
                        "$lt": 50
                    }
                }).await.unwrap();

                assert_eq!(50, result.deleted_count);
                Ok(())
            }
        }

        let db_path = mk_db_path("test-delete");
        open_server_with_test(db_path.as_path(), Box::new(TestRunner)).await.unwrap();
    }

    #[tokio::test]
    async fn test_aggregation() {
        use mongodb::{
            bson::{Document, doc},
            Collection
        };

        struct TestRunner;

        #[async_trait::async_trait]
        impl Runner for TestRunner {

            async fn run(&self, client: mongodb::Client) -> Result<()> {
                const COUNT: usize = 50;
                let mut docs: Vec<Document> = Vec::with_capacity(COUNT);
                for i in 0..COUNT {
                    docs.push(doc! {
                        "_id": i as i64,
                        "x": i as i64,
                    });
                }

                let database = client.database("sample_mflix");
                let my_coll: Collection<Document> = database.collection("movies");
                my_coll.insert_many(docs).await.unwrap();

                // count
                let count = my_coll.count_documents(doc! {}).await.unwrap();

                assert_eq!(COUNT as u64, count);
                Ok(())
            }
        }

        let db_path = mk_db_path("test-aggregation");
        let _ = std::fs::remove_dir_all(db_path.as_path());

        open_server_with_test(db_path.as_path(), Box::new(TestRunner)).await.unwrap();
    }

    #[tokio::test]
    async fn test_session() {
        use mongodb::{
            bson::{Document, doc},
            options::{ReadConcern, WriteConcern},
        };

        struct TestRunner;

        #[async_trait::async_trait]
        impl Runner for TestRunner {

            async fn run(&self, client: mongodb::Client) -> Result<()> {
                let mut session = client.start_session().await.unwrap();

                session
                    .start_transaction()
                    .read_concern(ReadConcern::majority())
                    .write_concern(WriteConcern::majority())
                    .await?;

                let coll = client.database("sample_mflix").collection::<Document>("movies");

                coll.insert_one(doc! { "x": 1 }).session(&mut session).await?;
                let one = coll.find_one(doc! { "x": 1 }).session(&mut session).await?;
                assert_eq!(1, one.unwrap().get_i32("x").unwrap());
                coll.delete_one(doc! { "x": 1 }).session(&mut session).await?;
                let one = coll.find_one(doc! { "x": 1 }).session(&mut session).await?;
                assert_eq!(None, one);

                session.commit_transaction().await?;

                let mut session = client.start_session().await.unwrap();

                session
                    .start_transaction()
                    .read_concern(ReadConcern::majority())
                    .write_concern(WriteConcern::majority())
                    .await?;

                coll.insert_one(doc! { "x": 1 }).session(&mut session).await?;
                session.abort_transaction().await?;

                Ok(())
            }
        }

        let db_path = mk_db_path("test-session");
        open_server_with_test(db_path.as_path(), Box::new(TestRunner)).await.unwrap();
    }

}