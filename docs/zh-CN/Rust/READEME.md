
# PoloDB Rust 手册

# 安装

```toml
[dependencies]
polodb_core = "0.3.0"
polodb_bson = "0.2.0"
```

# 使用

## 打开数据库

```rust
let mut db = Database::open(db_path.as_path().to_str().unwrap())?;
```

## 使用 Collection

创建
```rust
let mut collection = db.create_collection("test")?;
```

使用
```rust
let mut collection = db.collection("test")?;
```

## Count

```rust
let count = collection.count()?;
```

## Insert

```rust
let new_doc = mk_document! {
                "name": "Vincent Chan",
            };
collection.insert(Rc::new(new_doc)).unwrap();
```

## Find

```rust
let result = collection.find(
            Some(mk_document! {
                "content": "3",
            }.borrow())
        )?;
```

# Update

```
collection.update(...);
```

# Delete

```
collection.delete(...);
collection.delete_all();
```

# 事务

```rust
db.start_transaction(None).unwrap();
let mut collection = db.create_collection("test").unwrap();

// something

db.commit().unwrap()
```
