
# PoloDB

[![Build Status](https://travis-ci.com/vincentdchan/PoloDB.svg?branch=master)](https://travis-ci.com/vincentdchan/PoloDB)
[![Crates.io](https://img.shields.io/crates/v/polodb_core.svg)](https://crates.io/crates/polodb_core)
[![PYPI version](https://img.shields.io/pypi/v/polodb.svg)](https://pypi.org/project/polodb/)

PoloDB is an embedded JSON-based database.

[中文版](README_CN.md)

# Features

- Simple and Lightweight
  - Only cost ~500kb memory to serve a database
  - The database server binary is less than 2Mb 
- Easy to learn and use
  - NoSQL
  - MongoDB-like API
- Various language bindings
- Can be embedded or standalone
  - Embedded Mode
    - No standalone processes
    - No cross-process calls
    - No runtime dependency
  - Standalone: Run as a process, communicate with IPC
- Cross-Platform
- Multiple backends
  - Filesystem(WAL)
  - Memory

## Filesystem Backend

With the filesystem backend, PoloDB stores data in ONE file.
All the data are saved persistently on the disk.

It's designed to be flexible, universal, and easy to be searched.
All the data are encoded in [msgpack](https://msgpack.org/) format and stored in the PoloDB's btree format.

PoloDB uses WAL(write-ahead logging) to implement transactional writing and protect your data from program crashes.

## Memory Backend

With the memory backend, all the data all stored in memory, making PoloDB a pure memory database.

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

# Platform

Theoretically, PoloDB supports all platforms that the Rust compiler
supports.
But PoloDB is a personal project currently.
Limited by my time, I have only compiled and tested on the following platforms:

- macOS Big Sur x64
- Linux x64 (Tested on Fedora 32)
- Windows 10 x64

# Manual

- [Rust](https://docs.rs/polodb_core)
- [Node.js](https://github.com/vincentdchan/polodb.js)
- [Python](./docs/en-US/Python/READEME.md)

# Developing Plan

The features will be implemented one by one in order.

- [x] Basic database API
  - [x] CRUD
  - [x] Transactions
  - [ ] Indexes
  - [ ] Aggregation
- [x] Command line Tools
- [x] Language bindings
  - [x] C/C++
  - [x] Python ([Doc](./docs/en-US/Python/READEME.md))
  - [x] [Node.js](https://github.com/vincentdchan/polodb.js)
- [ ] Multi-threads support
- [ ] Extension API
  - [ ] Data Encryption
  - [ ] JavaScript Engine
- [ ] Visualization Tools

# V1 to V2

PoloDB V2 uses [msgpack](https://msgpack.org/) to encode data.

PoloDB V1 uses custom format.

## Migration

### Rust

`Database::v1_to_v2` can be used to migrate from v1 to v2.

### Tool

```sh
polodb migrate --target v2 ./mydb
```

# Contribute

| Module | Path | Description |
| -------| ---- | ----------- |
| Core | `src/polodb_core`  | The core implementation of the Database |
| C Library | `src/polodb_clib` | The C FFI implementation of PoloDB |
| CLI tool | `src/polodb` | The command line tool of PoloDB |
| [Node.js](https://github.com/vincentdchan/polodb.js) | `polodb.js` | The Node.js binding using N-API |
| Python bindings | `pypolodb` | The CPython binding using Extension API |
