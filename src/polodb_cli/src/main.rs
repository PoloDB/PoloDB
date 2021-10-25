mod dumper;

use std::sync::Mutex;
use std::rc::Rc;
use std::cell::RefCell;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use quick_js::{Context, JsValue};
use polodb_core::Database;

use clap::{Arg, App};
use crate::dumper::dump;

fn value_to_str(val: &JsValue) -> String {
    match val {
        JsValue::String(str) => {
            let mut result = String::new();
            result.push_str("\"");
            result.push_str(str);
            result.push_str("\"");
            result
        },

        JsValue::Int(i) => i.to_string(),

        JsValue::Null => "null".into(),

        JsValue::Array(arr) => {
            let mut result = String::new();
            result.push_str("[ ");
            for (index, item) in arr.iter().enumerate() {
                result.push_str(value_to_str(item).as_str());

                if index != arr.len() - 1 {
                    result.push_str(", ");
                }
            }
            result.push_str(" ]");

            result
        }

        JsValue::Bool(bl) => {
            if *bl {
                "true".into()
            } else {
                "false".into()
            }
        }

        JsValue::Float(f) => {
            f.to_string()
        }

        JsValue::Object(obj) => {
            let mut result = String::new();
            result.push_str("{ ");
            for (index, item) in obj.iter().enumerate() {
                result.push_str(item.0);
                result.push_str(": ");
                result.push_str(value_to_str(item.1).as_str());

                if index != obj.len() - 1 {
                    result.push_str(", ");
                }
            }
            result.push_str(" }");

            result
        }

        _ => unimplemented!()

    }
}

fn main() {
    let matches = App::new("PoloDB Cli")
        .version("1.0")
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
            .about("attach the database")
            .arg(Arg::with_name("path").index(1).required(true)))
        .get_matches();

    if let Some(sub) = matches.subcommand_matches("dump") {
        let path = sub.value_of("path").expect("no input path");
        let detail = sub.is_present("detail");
        dump(path, detail);
        return;
    }

    if let Some(sub) = matches.subcommand_matches("attach") {
        let path = sub.value_of("path").expect("no path");

        let db = Rc::new(RefCell::new(Database::open_file(path).expect("open database failed")));
        let context = Context::new().unwrap();

        {
            let db = Mutex::new(db);
            context.add_callback("__create_collection",  move |name: String| {
                let db = db.lock().unwrap();
                let mut db = db.as_ref().borrow_mut();
                db.create_collection(name.as_str()).unwrap();
                JsValue::Null
            }).unwrap();
        }

        {
            context.add_callback("exit",  move || {
                std::process::exit(0);
                #[allow(unreachable_code)]
                JsValue::Null
            }).unwrap();
        }

        {
            context.add_callback("__version",  move || {
                Database::get_version()
            }).unwrap();
        }

        context.eval(r#"
          var db = (function() {
            const collectionIdSymbol = Symbol("collectionIdSymbol");

            class Collection {

              constructor(id) {
                this[collectionIdSymbol] = id;
              }

              find() {
                return {
                  _id: "haha",
                };
              }

            }

            return {

              createCollection(name) {
                __create_collection(name);
              },

              getCollection(name) {
                return new Collection(1);
              },

              getVersion() {
                return __version();
              }

            };
          })();
        "#).unwrap();

        // `()` can be used when no completer is required
        let mut rl = Editor::<()>::new();
        loop {
            let readline = rl.readline(">> ");
            match readline {
                Ok(line) => {
                    rl.add_history_entry(line.as_str());
                    // println!("Line: {}", line);

                    let value = context.eval(line.as_str()).unwrap();
                    let str = value_to_str(&value);
                    println!("{}", str);
                },
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break
                },
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break
                },
                Err(err) => {
                    println!("Error: {:?}", err);
                    break
                }
            }
        }
    }
}
