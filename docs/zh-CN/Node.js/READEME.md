
# PoloDB 的 Node.js 版本

## 安装

Npm:

```
npm install --save polodb
```

Yarn:

```
yarn add polodb
```

## 打开一个数据库

避免数据丢失，记得关闭数据库。

```javascript
const polodb = require('polodb');

let db;
try {
    db = new polodb.Database("./test.db");
} catch (err) {
    console.log(err);
    // handle err
} finally {
    if (db) {
        db.close();
        db = undefined;
    }
}

```

## 创建一个数据集


```javascript

db.createCollection('students');

```

## 往数据集插入一条数据


```javascript
const collection = db.collection('students');

collection.insert({
    name: 'Vincent Chan',
    age: 14,
});

```

## 查找数据

```javascript
const collection = db.collection('students');

const result = collection.find({
    name: 'Vincent Chan',
});

console.log(result);
```

## 更新数据

```javascript
const collection = db.collection('students');

collection.update({
    name: 'Vincent Chan',
}, {
    '$inc': {
        age: 1,
    },
});
```

## 删除数据

删除符合查找条件的数据。

```javascript
const collection = db.collection('students');

collection.delete({
    name: 'Vincent Chan',
});

```

删除数据集里面的所有数据。

```javascript
const collection = db.collection('students');
collection.deleteAll();

```

## 事务

```javascript
db.startTransaction();
db.commit();
db.rollback();
```
