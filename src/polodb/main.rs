mod dumper;
mod ipc;

use crate::dumper::dump;
use crate::ipc::{IPC, Connection};
use polodb_core::Database;
use clap::{Arg, App};
use std::process::exit;
use std::io::{Read, Write};
use std::sync::Arc;
use std::thread;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, Ordering};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use error_chain::error_chain;
#[cfg(unix)]
use signal_hook::{iterator::Signals};
#[cfg(unix)]
use signal_hook::consts::TERM_SIGNALS;

static CONN_COUNT: AtomicI32 = AtomicI32::new(0);

error_chain! {

    foreign_links {
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
const PING_HEAD: [u8; 4] = [0xFF, 0x00, 0xAA, 0xCC];

#[derive(Clone)]
struct AppContext {
    socket_path: String,
    db: Arc<Mutex<Option<Database>>>,
}

impl AppContext {

    fn new(socket_path: String, db: Database) -> AppContext {
        AppContext {
            socket_path,
            db: Arc::new(Mutex::new(Some(db))),
        }
    }

    fn handle_incoming_connection(&self, conn: &mut Connection) -> Result<bool> {
        let mut db_guard = self.db.lock().unwrap();
        let db = db_guard.as_mut().unwrap();
        let mut header_buffer = [0u8; 4];

        conn.read_exact(&mut header_buffer)?;

        if header_buffer != HEAD {
            if header_buffer == PING_HEAD {
                let req_id = conn.read_u32::<BigEndian>()?;
                conn.write(&PING_HEAD)?;
                conn.write_u32::<BigEndian>(req_id)?;
                return Ok(true);
            }
            eprintln!("head is not matched, received: {:#x} {:#x} {:#x} {:#x}",
                      header_buffer[0], header_buffer[1], header_buffer[2], header_buffer[3]);
            eprintln!("exit");
            return Ok(false)
        }

        let req_id = conn.read_u32::<BigEndian>()?;

        let mut ret_buffer = Vec::new();

        let msg_ty_result = db.handle_request(conn, &mut ret_buffer);
        if let Err(err) = msg_ty_result {
            eprintln!("io error, exit: {}", err);
            return Ok(false);
        }

        let msg_ty = msg_ty_result.unwrap();

        conn.write(&HEAD)?;
        conn.write_u32::<BigEndian>(req_id)?;
        conn.write(&ret_buffer)?;
        conn.flush()?;

        if msg_ty.is_quit {
            return Ok(false);
        }

        Ok(true)
    }

}

fn start_socket_server(path: Option<&str>, socket_addr: &str) {
    let db = match path {
        Some(path) => {
            match Database::open_file(path) {
                Ok(db) => db,
                Err(err) => {
                    eprintln!("open db {} failed: {}", path, err);
                    exit(6);
                }
            }
        },
        None => {
            match Database::open_memory() {
                Ok(db) => db,
                Err(err) => {
                    eprintln!("open memory db failed: {}", err);
                    exit(6);
                }
            }
        }
    };

    let app = AppContext::new(socket_addr.into(), db);

    start_app_async(app.clone(), socket_addr);

    #[cfg(unix)]
    let mut signals = Signals::new(TERM_SIGNALS).unwrap();
    #[cfg(unix)]
    for _ in signals.forever() {
        eprintln!("Received quit signal, prepare to exit");
        safely_quit(app.clone());
    }
}


fn start_app_async(app: AppContext, socket_addr: &str) {
    let socket_attr_copy: String = socket_addr.into();
    let _t = thread::spawn(move || {
        let listener = IPC::bind(socket_attr_copy.as_str()).unwrap();

        for stream in listener.incoming() {
            let stream = stream.unwrap();

            eprintln!("Connection established!");
            CONN_COUNT.fetch_add(1, Ordering::SeqCst);
            let app = app.clone();
            thread::spawn(move || {
                let mut moved_stream = stream;
                loop {
                    let result = app.handle_incoming_connection(&mut moved_stream);
                    match result {
                        Ok(true) => {
                            continue;
                        },

                        Ok(false) => {
                            safely_quit(app.clone());
                        },

                        Err(err) => {
                            match err.0 {
                                ErrorKind::Io(_) => {
                                    eprintln!("io error: {}", err);
                                    break;
                                }
                                _ => {
                                    eprintln!("other error, continue: {}", err);
                                }
                            }
                        }
                    }
                }
                if CONN_COUNT.fetch_sub(1, Ordering::SeqCst) <= 1 {
                    eprintln!("no connection, quit");
                    safely_quit(app.clone());
                }
            });
        }
    });

    #[cfg(windows)]
    _t.join().unwrap();
}

fn safely_quit(app: AppContext) {
    let mut db_guard = app.db.lock().unwrap();
    *db_guard = None;
    let _ = std::fs::remove_file(&app.socket_path);
    eprintln!("safely exit");
    exit(0);
}

fn main() {
    let version = Database::get_version();
    let app = App::new("PoloDB")
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
            start_socket_server(Some(path), socket);
        } else if sub.is_present("memory") {
            start_socket_server(None, socket);
        } else {
            eprintln!("you should pass either --path or --memory");
        }
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
