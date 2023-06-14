/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod ipc;
mod server;

use polodb_core::Database;
use clap::{Arg, Command as App};
use thiserror::Error;

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

fn main() {
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
        .subcommand(App::new("migrate")
            .about("migrate the older database to the newer format")
            .arg(
                Arg::new("path")
                    .index(1)
                    .required(true)
            )
            .arg(
                Arg::new("target")
                    .long("target")
                    .value_name("TARGET")
                    .num_args(0..=1)
                    .required(true)
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
            server::start_socket_server(Some(path), socket);
        } else if sub.contains_id("memory") {
            server::start_socket_server(None, socket);
        } else {
            eprintln!("you should pass either --path or --memory");
        }
        return;
    }

    // println!("{}", matches.usage());
}
