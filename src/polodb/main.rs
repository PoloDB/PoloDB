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

mod wire;
mod bson_util;
mod compression;
pub(crate) mod sync_read_ext;
pub(crate) mod checked;
mod reply;
mod handlers;
mod app_context;

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use polodb_core::Database;
use bson::{rawdoc};
use clap::{Arg, Command as App};
use anyhow::Result;
use tokio::io::{AsyncRead, AsyncWrite};
use log::{info, warn, error, debug};
use tokio::select;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use reply::Reply;
use crate::app_context::AppContext;
use crate::handlers::{DeleteHandler, FindHandler, GetMoreHandler, HelloHandler, InsertHandler, KillCursorsHandler, UpdateHandler};

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

    ctx.push_handler(FindHandler::new());
    ctx.push_handler(GetMoreHandler::new());
    ctx.push_handler(KillCursorsHandler::new());
    ctx.push_handler(InsertHandler::new());
    ctx.push_handler(UpdateHandler::new());
    ctx.push_handler(DeleteHandler::new());
    ctx.push_handler(HelloHandler::new());

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
                                let result = handle_stream(ctx, conn_id, stream).await;
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
    Ok(())
}

async fn handle_message<W: AsyncWrite + Unpin>(ctx: AppContext, conn_id: u64, stream: &mut W, message: wire::Message) -> Result<()> {
    let handler = ctx.get_handlers(&message.document_payload)?;
    if let Some(handler) = handler {
        let reply = handler.handle(ctx.clone(), conn_id, &message).await?;
        reply.write_to(stream).await?;
    } else {
        let doc = rawdoc! {
            "ok": 0,
            "errmsg": "no handler found",
        };
        let reply = Reply::new(message.request_id.unwrap(), doc);
        reply.write_to(stream).await?;
    }
    Ok(())
}

pub fn mk_db_path(db_name: &str) -> PathBuf {
    let mut db_path = env::temp_dir();
    let db_filename = String::from(db_name) + "-db-server";
    db_path.push(db_filename);
    db_path
}

#[tokio::test]
async fn test_server() {
    use mongodb::{
        bson::{Document, doc},
        Client,
        Collection
    };

    env::set_var("RUST_LOG", "polodb=debug,tokio=info, mongodb=debug");
    let _ = env_logger::try_init();

    let db_path = mk_db_path("test-server");
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
        let database = client.database("sample_mflix");
        let my_coll: Collection<Document> = database.collection("movies");
        my_coll.insert_one(doc! { "x": 1 }).await.unwrap();
    }

    token.cancel();
    handle.await.unwrap()
}

#[tokio::test]
async fn test_find() {
    use mongodb::{
        bson::{Document, doc},
        Client,
        Collection
    };

    use futures::TryStreamExt;
    env::set_var("RUST_LOG", "polodb=debug,tokio=info, mongodb=debug");
    let _ = env_logger::try_init();

    let db_path = mk_db_path("test-find");
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

        let mut all = Vec::<Document>::new();

        while let Some(doc) = cursor.try_next().await.unwrap() {
            all.push(doc);
        }

        assert_eq!(1000, all.len());
    }

    token.cancel();
    handle.await.unwrap()
}

#[tokio::test]
async fn test_cursor_drop() {
    use mongodb::{
        bson::{Document, doc},
        Client,
        Collection
    };

    use futures::TryStreamExt;
    env::set_var("RUST_LOG", "polodb=debug,tokio=info, mongodb=debug");
    let _ = env_logger::try_init();

    let db_path = mk_db_path("test-find");
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
        Client,
        Collection
    };

    env::set_var("RUST_LOG", "polodb=debug,tokio=info, mongodb=debug");
    let _ = env_logger::try_init();

    let db_path = mk_db_path("test-update");
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
    }
    // sleep
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // wait for killCursors command to be sent
    // no crash is success

    token.cancel();
    handle.await.unwrap()
}

#[tokio::test]
async fn test_delete() {
    use mongodb::{
        bson::{Document, doc},
        Client,
        Collection
    };

    env::set_var("RUST_LOG", "polodb=debug,tokio=info, mongodb=debug");
    let _ = env_logger::try_init();

    let db_path = mk_db_path("test-delete");
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
    }
    // sleep
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // wait for killCursors command to be sent
    // no crash is success

    token.cancel();
    handle.await.unwrap()
}
