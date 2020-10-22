
# PoloDB

PoloDB is a embedded JSON-based database.

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

# Developing Plan

The feature will be implemented one by on in order.

- [x] Basic database API
  - [x] CURD
  - [x] Transactions
  - [ ] Indexes
  - [ ] Join operations
- [x] Command line Tools
- [ ] Mobile Platform Compilation
  - [ ] iOS
  - [ ] Android
  - [ ] Flutter
- [ ] Language bindings
  - [x] C/C++
  - [ ] Go
  - [ ] Swift
  - [x] Python
  - [x] Node.js ([Doc](./docs/en-US/Node.js/READEME.md))
  - [ ] Java/Kotlin
  - [ ] Dart
- [ ] Tons of tests
- [ ] Multi-threads support
- [ ] Extension API
  - [ ] Data Encryption
  - [ ] JavaScript Engine
- [ ] Visualization Tools

# Languages

- [Rust](./docs/en-US/Rust/READEME.md)
- [Node.js](./docs/en-US/Node.js/READEME.md)
- [Python](WIP)

# Command Line Tool

The core part of PoloDB has no dependency. But PoloDB provides
a standalone bool to help user handle the database interactively.

The command line tool is based on [QuickJS](https://bellard.org/quickjs/),
which provides a full-feature JavaScript enverionment.

# FAQ

## Does PoloDB support multi-threads?

Currently **NOT**. It's already in the developing plan.
