
# PoloDB

PoloDB is a embedded JSON-based database.

[中文版](README_CN.md)

# Feature

- Embedded(No standalone processes, no cross-process calls)
- No dependency(except for libc, which is a system lib)
- NoSQL
- MongoDB like API
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

# Developing Plan

The feature will be implemented one by on in order.

- [x] Basic database API
- [x] Command line Tools
- [ ] Mobile Platform Compilation
  - [ ] iOS
  - [ ] Android
  - [ ] Flutter
- [ ] Language bindings
  - [x] C
  - [ ] Go
  - [ ] Objective-C
  - [ ] Swift
  - [x] Python
  - [ ] Ruby
  - [ ] Elixir
  - [x] Node.js
  - [ ] Java
  - [ ] Kotlin
  - [ ] Dart
- [ ] Tons of tests
- [ ] Multi-threads support
- [ ] Extension API
  - [ ] Data Encryption
  - [ ] JavaScript Engine
- [ ] Visualization Tools

# Command Line Tool

The core part of PoloDB has no dependency. But PoloDB provides
a standalone bool to help user handle the database interactively.

The command line tool is based on [QuickJS](https://bellard.org/quickjs/),
which provides a full-feature JavaScript enverionment.

# FAQ

## Does PoloDB support multi-threads?

Currently **NOT**. It's already in the developing plan.
