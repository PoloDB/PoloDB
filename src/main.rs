
mod bson;
mod btree;
mod page;
mod journal;
mod vm_code;
mod vm;
mod db;

use bson::ObjectId;

fn main() {
    let _id = ObjectId { timestamp: 0, counter: 0 };
    println!("Hello, world!");
}
