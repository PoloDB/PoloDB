/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use polodb_core::Database;
use clap::{Arg, Command as App};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Error, Debug)]
pub enum Error {
    #[error("db error: {0}")]
    Db(#[from] polodb_core::Error),
    #[error("bson error: {0}")]
    BsonDe(#[from] polodb_core::bson::de::Error),
    #[error("io error: {0}")]
    Io(#[from]::std::io::Error),
    #[error("unwrap request body failed")]
    RequestBodyNotFound,
}

pub type Result<T> = std::result::Result<T, Error>;

#[tokio::main]
async fn main() {
    let version = Database::get_version();
    let app = App::new("PoloDB")
        .version(version)
        .about("Command line tool for PoloDB")
        .author("Vincent Chan <okcdz@diverse.space>")
        .subcommand(App::new("serve")
            .about("attach the database, start the tcp server")
            .arg(
                Arg::new("socket")
                    .short('s')
                    .long("socket")
                    .help("the domain socket to listen on").required(true)
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

        let socket = sub.get_one::<String>("socket").unwrap();
        let path = sub.get_one::<String>("path");
        if let Some(path) = path {
            start_socket_server(path.clone(), socket.to_string()).await.unwrap();
        } else {
            eprintln!("you should pass --path ");
        }
        return;
    }

}

async fn start_socket_server(path: String, socket: String) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::UnixListener::bind(&socket)?;
    println!("listening on {}", socket);

    while let Ok((mut stream, _)) = listener.accept().await {
        let path = path.clone();
        tokio::spawn(async move {
            let db = Database::open_file(&path).unwrap();
            let db = std::sync::Arc::new(db);
            let db = db.clone();

            let mut buf = [0; 1024];

            // In a loop, read data from the socket and write the data back.
            loop {
                let n = match stream.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                // Write the data back
                if let Err(e) = stream.write_all(&buf[0..n]).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }
        });
    }

    Ok(())
}
