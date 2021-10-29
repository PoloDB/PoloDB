mod dumper;

use crate::dumper::dump;
use polodb_core::Database;
use clap::{Arg, App};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::process::exit;
use std::io::{Read, Write};
use std::sync::Arc;
use std::thread;
use std::sync::Mutex;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use error_chain::error_chain;

error_chain! {

    foreign_links {
        Bson( polodb_bson::BsonErr);
        Db( polodb_core::DbErr);
        Fmt(::std::fmt::Error);
        Io(::std::io::Error);
    }

    errors {
        UnwrapDocument {
            display("unwrap db failed")
        }
        UnwrapFail(str: String) {
            display("unwrap value failed: {}", str)
        }
    }

}

const HEAD: [u8; 4] = [0xFF, 0x00, 0xAA, 0xBB];

#[derive(Clone)]
struct AppContext {
    db: Arc<Mutex<Database>>,
}

impl AppContext {

    fn new(db: Database) -> AppContext {
        AppContext {
            db: Arc::new(Mutex::new(db)),
        }
    }

    fn handle_incoming_connection(&self, mut conn: TcpStream) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        let mut header_buffer = [0u8; 4];

        conn.read_exact(&mut header_buffer)?;

        if header_buffer != HEAD {
            conn.shutdown(Shutdown::Both)?;
            return Ok(())
        }

        let req_id = conn.read_u64::<BigEndian>()?;

        let mut ret_buffer = Vec::new();

        db.handle_request(&mut conn, &mut ret_buffer);

        conn.write(&HEAD)?;
        conn.write_u64::<BigEndian>(req_id)?;
        conn.write(&ret_buffer)?;

        Ok(())
    }

}

fn start_tcp_server(path: &str, listen: &str) {
    let db = match Database::open_file(path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("open db failed: {}", err);
            exit(6);
        }
    };

    let app = AppContext::new(db);

    let listener = TcpListener::bind(listen).unwrap();
    // let pool = ThreadPool::new();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        println!("Connection established!");
        let app = app.clone();
        thread::spawn(move || {
            let result = app.handle_incoming_connection(stream);
            if let Err(err) = result {
                eprintln!("error: {}", err);
            }
        });
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
