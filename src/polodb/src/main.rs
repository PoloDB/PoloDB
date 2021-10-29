mod dumper;

use std::net::{TcpListener, TcpStream, Shutdown};
use polodb_core::Database;
use clap::{Arg, App};
use crate::dumper::dump;
use std::process::exit;
use std::io::Read;

const HEAD: [u8; 4] = [0xFF, 0x00, 0xAA, 0xBB];

fn handle_incoming_connection(conn: &mut TcpStream, _db: &mut Database) {
    let mut header_buffer = [0u8; 4];

    conn.read_exact(&mut header_buffer).unwrap();

    if header_buffer != HEAD {
        conn.shutdown(Shutdown::Both);
    }

    unimplemented!()
}

fn start_tcp_server(path: &str, listen: &str) {
    let mut db = match Database::open_file(path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("open db failed: {}", err);
            exit(6);
        }
    };

    let listener = TcpListener::bind(listen).unwrap();

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();

        println!("Connection established!");
        handle_incoming_connection(&mut stream, &mut db);
    }
}

fn main() {
    let version = Database::get_version();
    let app = App::new("PoloDB Cli")
        .version(version.as_str())
        .about("Command line tool for PoloDB")
        .author("Vincent Chan <okcdz@diverse.space>")
        .subcommand(App::new("dump")
            .about("dump the database to text")
            .arg(
                Arg::with_name("path")
                    .index(1)
                    .required(true)
            )
            .arg(Arg::with_name("detail").required(false)))
        .subcommand(App::new("attach")
            .about("attach the database, start the tcp server")
            .arg(Arg::with_name("path").index(1).required(true)))
            .arg(Arg::with_name("listen").help("the address to listen on").required(true))
        ;

    let matches = app.get_matches();

    if let Some(sub) = matches.subcommand_matches("attach") {
        let path = sub.value_of("path").expect("no input path");
        let server = sub.value_of("listen").unwrap();
        start_tcp_server(path, server);
        return;
    }

    if let Some(sub) = matches.subcommand_matches("dump") {
        let path = sub.value_of("path").expect("no input path");
        let detail = sub.is_present("detail");
        dump(path, detail);
        return;
    }

    println!("{}", matches.usage());
}
