<p align="center">
  <img src="./images/brand.png" alt="PoloDB" width="480" />
</p>

<p align="center">
  An embedded document database for Rust with a MongoDB-like API.
</p>

<p align="center">
  <a href="https://github.com/PoloDB/PoloDB/actions/workflows/rust.yml"><img src="https://github.com/PoloDB/PoloDB/actions/workflows/rust.yml/badge.svg" alt="CI" /></a>
  <a href="https://crates.io/crates/polodb_core"><img src="https://img.shields.io/crates/v/polodb_core.svg" alt="Crates.io" /></a>
  <a href="https://docs.rs/polodb_core"><img src="https://docs.rs/polodb_core/badge.svg" alt="docs.rs" /></a>
  <a href="https://github.com/PoloDB/PoloDB/releases/latest"><img src="https://img.shields.io/github/v/release/PoloDB/PoloDB" alt="GitHub release" /></a>
  <a href="LICENSE.txt"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="Apache-2.0 license" /></a>
</p>

PoloDB stores BSON documents locally and exposes familiar collection, query,
update, index, aggregation, and transaction APIs. It can be used as an
embedded Rust library or run as a standalone server with partial MongoDB wire
protocol compatibility.

## Features

- Embedded, typed Rust API built around `serde` and BSON
- MongoDB-like CRUD, query, update, index, and aggregation APIs
- Explicit transactions and automatic per-operation transactions
- Concurrent use through clonable database handles
- RocksDB-backed persistent storage
- Standalone server for supported MongoDB driver operations
- Experimental Python bindings built from the same Rust core

## Project status

The latest stable release is
[v5.2.0](https://github.com/PoloDB/PoloDB/releases/tag/v5.2.0).
See the [changelog](CHANGELOG.md) for release details.

PoloDB v5 uses RocksDB as its storage backend. A database path is a directory,
not a single portable database file. The first build compiles the bundled
RocksDB sources and can take several minutes.

Rust CI currently covers Ubuntu x64, Ubuntu ARM64, macOS, and Windows. Python
bindings are tested on Python 3.12 through 3.14. Indexes, aggregation, the
standalone server, and language bindings continue to evolve; test the behavior
your application depends on before using PoloDB in production.

## Installation

Add the embedded library and Serde to your project:

```toml
[dependencies]
polodb_core = "5.2.0"
serde = { version = "1", features = ["derive"] }
```

Building requires a Rust toolchain, a C/C++ build toolchain, and Clang/libclang
for the bundled RocksDB bindings.

## Quick start

```rust
use polodb_core::{bson::doc, CollectionT, Database};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Book {
    title: String,
    author: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open_path("./data/books")?;
    let books = db.collection::<Book>("books");

    books.insert_one(Book {
        title: "The Three-Body Problem".to_string(),
        author: "Liu Cixin".to_string(),
    })?;

    let book = books.find_one(doc! {
        "author": "Liu Cixin",
    })?;

    println!("{book:?}");
    Ok(())
}
```

See the [PoloDB documentation](https://www.polodb.org/docs) and
[`polodb_core` API documentation](https://docs.rs/polodb_core) for queries,
updates, indexes, aggregation, and transactions.

## Standalone server

Install and start the server:

```console
cargo install polodb --version 5.2.0
polodb serve --path ./data/server --host 127.0.0.1 --port 27017
```

The server implements the subset of the MongoDB wire protocol used by its
supported commands. It is not a drop-in replacement for a full MongoDB server.

## Packages

| Package | Status | Purpose |
| --- | --- | --- |
| [`polodb_core`](https://crates.io/crates/polodb_core) | Published | Embedded Rust database library |
| [`polodb`](https://crates.io/crates/polodb) | Published | Standalone server and CLI |
| [`py-polodb`](py-polodb/README.md) | Experimental | Python bindings developed in this repository |

Release binaries for the standalone server are published for macOS x64, Linux
x64, and Windows x64 on the
[GitHub Releases page](https://github.com/PoloDB/PoloDB/releases).

## Roadmap and support

Open work is tracked in [GitHub Issues](https://github.com/PoloDB/PoloDB/issues).
Mobile SDKs, additional language bindings, encryption, multikey indexes, and
alternative storage backends require further design and are not part of the
current stable feature set.

- [Documentation](https://www.polodb.org/docs)
- [Rust API](https://docs.rs/polodb_core)
- [Issues](https://github.com/PoloDB/PoloDB/issues)
- [Discord](https://discord.gg/NmGQyVx6hH)

## License

PoloDB is licensed under the [Apache License 2.0](LICENSE.txt).
