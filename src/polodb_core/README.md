# PoloDB Core

`polodb_core` is an embedded document database for Rust with a MongoDB-like
API. It stores BSON documents in a local RocksDB-backed directory and does not
require a separate database process.

## Installation

```console
cargo add polodb_core
cargo add serde --features derive
```

PoloDB v5 builds its bundled RocksDB backend from source. The first build can
take several minutes and requires:

- a Rust toolchain
- a C/C++ build toolchain
- Clang and libclang
- platform C headers

On Windows, the project is continuously tested with the MSVC toolchain on
GitHub Actions. MinGW is not currently part of the supported CI matrix.

## Package distinction

- `polodb_core`: embedded Rust library
- `polodb`: standalone server and CLI; install it with `cargo install polodb`

PoloDB provides MongoDB-style APIs, but does not implement every MongoDB query,
index, aggregation, or wire-protocol feature. Test the behavior your application
depends on before production use.

- [Project repository](https://github.com/PoloDB/PoloDB)
- [Documentation](https://www.polodb.org/docs)
- [Rust API](https://docs.rs/polodb_core)
