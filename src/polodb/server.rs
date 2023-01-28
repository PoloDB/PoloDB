use std::thread;
use std::sync::atomic::{AtomicI32, Ordering};
use polodb_core::Database;
use std::process::exit;
use std::sync::{Arc, Mutex};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use crate::ErrorKind;
use crate::ipc::{Connection, IPC};
#[cfg(unix)]
use signal_hook::iterator::Signals;
#[cfg(unix)]
use signal_hook::consts::TERM_SIGNALS;

static CONN_COUNT: AtomicI32 = AtomicI32::new(0);

const HEAD: [u8; 4] = [0xFF, 0x00, 0xAA, 0xBB];
const PING_HEAD: [u8; 4] = [0xFF, 0x00, 0xAA, 0xCC];

#[derive(Clone)]
struct AppContext {
    socket_path: String,
    db: Arc<Mutex<Option<Arc<Database>>>>,
}

impl AppContext {

    fn new(socket_path: String, db: Database) -> AppContext {
        AppContext {
            socket_path,
            db: Arc::new(Mutex::new(Some(Arc::new(db)))),
        }
    }

    fn handle_incoming_connection(&self, conn: &mut Connection) -> crate::Result<bool> {
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

        // unwrap the db from app context
        let db = {
            // limit the scope of the guard because
            // the Database itself is threadsafe
            let db_guard = self.db.lock().unwrap();

            match db_guard.as_ref() {
                Some(db) => {
                    db.clone()
                }
                None => {
                    // if the database already is None, exit
                    return Ok(false);
                }
            }
        };
        let msg_ty_result = db.handle_request(conn);
        if let Err(err) = msg_ty_result {
            eprintln!("io error, exit: {}", err);
            return Ok(false);
        }

        let result = msg_ty_result.unwrap();
        let ret_buffer = polodb_core::bson::to_vec(&result.value).unwrap();

        conn.write(&HEAD)?;
        conn.write_u32::<BigEndian>(req_id)?;
        conn.write(&ret_buffer)?;
        conn.flush()?;

        if result.is_quit {
            return Ok(false);
        }

        Ok(true)
    }

}

pub fn start_socket_server(path: Option<&str>, socket_addr: &str) {
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
