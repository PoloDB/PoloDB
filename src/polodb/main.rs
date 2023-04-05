/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod ipc;
mod server;

use polodb_core::Database;
use clap::{App, Arg};
use error_chain::error_chain;

error_chain! {

    foreign_links {
        Db( polodb_core::DbErr);
        BsonDe(polodb_core::bson::de::Error);
        Fmt(::std::fmt::Error);
        Io(::std::io::Error);
    }

    errors {
        RequstBodyNotFound {
            display("unwrap request body failed")
        }
    }

}

fn main() {
    let version = Database::get_version();
    let app = App::new("PoloDB")
        .version(version.as_str())
        .about("Command line tool for PoloDB")
        .author("Vincent Chan <okcdz@diverse.space>")
        .subcommand(App::new("serve")
            .about("attach the database, start the tcp server")
            .arg(
                Arg::with_name("socket")
                    .short("s")
                    .long("socket")
                    .help("the domain socket to listen on").required(true)
                    .takes_value(true)
            )
            .arg(
                Arg::with_name("path")
                    .short("p")
                    .long("path")
                    .value_name("PATH")
                    .takes_value(true)
            )
            .arg(Arg::with_name("memory"))
            .arg(
                Arg::with_name("log")
                    .help("print log")
                    .long("log")
                    .short("l")
            )
        )
        .subcommand(App::new("migrate")
            .about("migrate the older database to the newer format")
            .arg(
                Arg::with_name("path")
                    .index(1)
                    .required(true)
            )
            .arg(
                Arg::with_name("target")
                    .long("target")
                    .value_name("TARGET")
                    .takes_value(true)
                    .required(true)
            )
        )
        .arg(
            Arg::with_name("log")
                .help("print log")
                .long("log")
                .short("l")
        );

    let matches = app.get_matches();

    if let Some(sub) = matches.subcommand_matches("serve") {
        let should_log = sub.is_present("log");
        Database::set_log(should_log);

        let socket = sub.value_of("socket").unwrap();
        let path = sub.value_of("path");
        if let Some(path) = path {
            server::start_socket_server(Some(path), socket);
        } else if sub.is_present("memory") {
            server::start_socket_server(None, socket);
        } else {
            eprintln!("you should pass either --path or --memory");
        }
        return;
    }

    println!("{}", matches.usage());
}
