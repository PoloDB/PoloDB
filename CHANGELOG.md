
# 4.4.2

- Fix #148

# 4.4.1

- Fix #143

# 4.4.0

- Feat(#114): $regex operator
- Feat(#118): $match aggregation
- Fix(#121): persist issue after second insert

# 4.3.2

- Fix index iteration issue

# 4.3.1

- Introduce `find_one()` API

# 4.3.0

- Introduce `create_index` API

# 4.2.0

- Introduce `thiserror` for error handling
- Rename `DbErr` to `Error`

# 4.1.1

- feat: preserve file size after reopen the database
- fix: `find()` empty collection
- fix: recycle free segments

# 4.1.0

- Use `Cursor<T>` api for `find()` method

# 4.0.1

- Implement IndexedDB backend(alpha)
- Validate collection's name

# 4.0.0

- **Breaking change:** use LSM-Tree as backend data structure
  to implement multiple writers

# 3.5.2

- Re-license to MPL-2.0
- Introduce WASM backend(WIP)

# 3.5.1

- Allow insert different primary keys into one collection
- Fix page lost in memory backend
- Fix page allocation error

# 3.5.0

- New KV storage engine

# 3.4.0

- Implement `ClientSession`

# 3.3.3

- Fix: insert_many transaction conflicts
- Fix: `$inc` and `$mul` for `i32` type
- bson ipc for Node.js

# 3.3.1

- Update package information
- Test release

# 3.3.0

- Add `drop()` to collection
- Add `list_collection_names()` to Database
- Database implements `Send` and `Sync`, allow manipuate in threads

# 3.2.0

- Add `update_one()` API
- Fix: `modified_count` is always `0` after updating
- Add `delete_one()` API
- Don't need `mut` for `Database` anymore

# 3.1.0

- Enrich the documentations
- Refactor the API of collection.
- Fix: locking issue on Windows

# 3.0.0

- **Breaking change**: Uses `bson` crate as data format
- MongoDB-like API

# 0.10.0

- feat(core): support storing very large documents
- fix(core): bugs of transactions
- fix(Python): bugs

# 0.9.0

- enhance(core): refactor codes
- fix(core): bugs of $push
- fix(core): some crashes
- feat(C): add new APIs

# 0.8.0

- feat(core): add find_all() API
- feat(core): add find_one() API
- feat(js): add findOne() API

# 0.7.0

- feat(core): logic `$or` and `$not`
- feat(core): array operation `$size`
- core: use crc64fast

# 0.6.0

- fix core bugs
- fix(js): object converion bug

# 0.5.1

- fix(core): bugs

# 0.4.3

- add more unit-tests
- fix(core): bugs
- fix(js): js binding crash
- feat(js): add UTCDateTime binding
