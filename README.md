
<img src="./images/brand.png" alt="" width="480" />

[![Crates.io](https://img.shields.io/crates/v/polodb_core.svg)](https://crates.io/crates/polodb_core)
[![docs.rs](https://docs.rs/polodb_core/badge.svg)](https://docs.rs/polodb_core)
[![License](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

PoloDB is an embedded JSON-based database.

| [Documentations](https://www.polodb.org/docs) | [中文版](README_CN.md) |

# Introduction

PoloDB is a library written in Rust
that implements a lightweight [MongoDB](https://www.mongodb.com/).

PoloDB has no dependency(except for libc),
so it can be easily run on most platforms (thanks
for Rust Language).

The data of PoloDB is stored in a file.
The file format is stable, cross-platform, and
backwards compatible.

The API of PoloDB is very similar to MongoDB.
It's very easy to learn and use.

# Features

- Simple and Lightweight
  - Only cost ~500kb memory to serve a database
  - The database server binary is less than 2Mb 
- Easy to learn and use
  - NoSQL
  - MongoDB-like API
- Various language bindings
- Embedded
  - No standalone processes
  - No cross-process calls
  - No runtime dependency
- Cross-Platform
- Multiple backends
  - Filesystem(WAL)
  - Memory

# Quick start

PoloDB is easy to learn and use:

```rust
use polodb_core::Database;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct Book {
    title: String,
    author: String,
}

let mut db = Database::open_file(db_path)?;
let mut collection = db.collection::<Book>("books");
collection.insert_one(Book {
    title: "The Three-Body Problem".to_string(),
    author: "Liu Cixin".to_string(),
})?;
```

# Backends

![](./images/backend.png)

## Filesystem Backend

With the filesystem backend, PoloDB stores data in ONE file.
All the data are saved persistently on the disk.

It's designed to be flexible, universal, and easy to be searched.
All the data are encoded in [bson](http://bsonspec.org/) format and stored in the PoloDB's btree format.

PoloDB uses WAL(write-ahead logging) to implement transactional writing and protect your data from program crashes.

## Memory Backend

With the memory backend, all the data all stored in memory, making PoloDB a pure memory database.

# Platform

Theoretically, PoloDB supports all platforms that the Rust compiler
supports.
But PoloDB is a personal project currently.
Limited by my time, I have only compiled and tested on the following platforms:

- macOS Big Sur x64
- Linux x64 (Tested on Fedora 32)
- Windows 10 x64

# Manual

- [Documentations](https://www.polodb.org/docs)
- [Rust](https://docs.rs/polodb_core)

# Roadmap

The features will be implemented one by one in order.

- [x] Basic database API
  - [x] CRUD
  - [x] Transactions
  - [x] Serde
  - [ ] Indexes
  - [ ] Aggregation
- [x] Command line Tools
- [ ] Language bindings
  - [ ] [Node.js](https://github.com/vincentdchan/polodb.js)
  - [ ] Python
- [ ] Multi-threads support
- [ ] Extension API
  - [ ] Data Encryption
  - [ ] JavaScript Engine
- [ ] Visualization Tools
