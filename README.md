
# PoloDB

[![Build Status](https://travis-ci.org/vincentdchan/PoloDB.svg?branch=master)](https://travis-ci.org/vincentdchan/PoloDB)
[![Crates.io](https://img.shields.io/crates/v/polodb_core.svg)](https://crates.io/crates/polodb_core)
[![npm version](https://img.shields.io/npm/v/polodb.svg)](https://www.npmjs.com/package/polodb)
[![PYPI version](https://img.shields.io/pypi/v/polodb.svg)](https://pypi.org/project/polodb/)

PoloDB is an embedded JSON-based database.

[中文版](README_CN.md)

# Features

- Simple/Lightweight/Easy to learn and use
- Various languages binding
- Embedded(No standalone processes, no cross-process calls)
- No runtime dependency
- NoSQL
- MongoDB-like API
- Cross-Platform
- Store data in ONE file

# Introduction

PoloDB is a library written in Rust
that implements a lightweight [MongoDB](https://www.mongodb.com/).

PoloDB has no dependency(except for libc),
so it can be easily run on most platform(thanks 
for Rust Language).

The data of PoloDB is stored in a file.
The file format is stable, cross-platform, and
backwards compaitible.

The API of PoloDB is very similar to MongoDB.
It's very easy to learn and use.

# Platform

Theoretically, PoloDB supports all platforms that Rust compiler
supports.
But PoloDB is a personal project currently.
Limited by my time, I only compile and test on the following platform:

- macOS 10.15 x64
- Linux x64 (Tested on Fedora 30)
- Windows 10 x64

# Manual

- [Rust](https://docs.rs/polodb_core)
- [Node.js](./docs/en-US/Node.js/READEME.md)
- [Python](./docs/en-US/Python/READEME.md)

# Developing Plan

The feature will be implemented one by on in order.

- [x] Basic database API
  - [x] CURD
  - [x] Transactions
  - [ ] Indexes
  - [ ] Aggregation
- [x] Command line Tools
- [x] Mobile Platform Compilation
  - [x] iOS
  - [ ] Android
  - [ ] Flutter
- [ ] Language bindings
  - [x] C/C++
  - [ ] Go
  - [x] Swift
  - [x] Python ([Doc](./docs/en-US/Python/READEME.md))
  - [x] Node.js ([Doc](./docs/en-US/Node.js/READEME.md))
  - [ ] Java/Kotlin
  - [ ] Dart
- [ ] Tons of tests
- [ ] Multi-threads support
- [ ] Extension API
  - [ ] Data Encryption
  - [ ] JavaScript Engine
- [ ] Visualization Tools

# Contribute

| Module | Path | Description |
| -------| ---- | ----------- |
| Core | `src/polodb_core`  | The core implementation of the Database |
| C Library | `src/polodb_clib` | The C FFI implementation of PoloDB |
| CLI tool | `src/polodb_cli` | The command line tool of PoloDB |
| Node.js bindings | `polodb.js` | The Node.js binding using N-API |
| Python bindings | `pypolodb` | The CPython binding using Extension API |
| Swift bindings | `SwiftyPoloDB` | The Swift binding |
