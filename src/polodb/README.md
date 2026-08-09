# PoloDB Server

`polodb` is the standalone PoloDB server and command-line application. It is a
binary package, not the crate to add as an embedded Rust dependency.

## Installation

Install the server from crates.io:

```console
cargo install polodb --version 5.3.0
```

Then start it with:

```console
polodb serve --path ./data/server --host 127.0.0.1 --port 27017
```

Prebuilt x64 binaries for macOS, Linux, and Windows are available on the
[GitHub Releases page](https://github.com/PoloDB/PoloDB/releases).

The server implements a subset of the MongoDB wire protocol used by its
supported commands. It is not a drop-in replacement for a full MongoDB server.

## Embedded Rust applications

For an embedded database with no separate server process, use `polodb_core`
instead:

```console
cargo add polodb_core
```

See the [project README](https://github.com/PoloDB/PoloDB) and
[documentation](https://www.polodb.org/docs) for prerequisites and examples.
