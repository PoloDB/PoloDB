
# PoloDB Node.js Client

[![npm version](https://img.shields.io/npm/v/polodb.svg)](https://www.npmjs.com/package/polodb)

[PoloDB](https://github.com/vincentdchan/PoloDB) is a lightweight JSON-based database.

## Features

- Simple and Lightweight
  - Only cost ~500kb memory to serve a database
  - The database server binary is less than 2Mb 
  - Store data in one file
- Easy to learn and use
  - NoSQL
  - MongoDB-like API
- Various language bindings
- Standalone Process
  - Process isolation
  - Asynchronous IO
- Cross-Platform
  - Working on common OS

## Install

Npm:

```
npm install --save polodb
```

Yarn:

```
yarn add polodb
```

## Open a database


```javascript
import { PoloDbClient } from 'polodb';

async function main() {
  const db = await PoloDbClient.createConnection('./test.db);
}

```

Remember to close the database.

```javascript
db.dispose();
```

## Create a Collection

Create a collection.

```javascript

await db.createCollection('students');

```

## Insert

Insert a document to the database.

```javascript
const collection = db.collection('students');

await collection.insert({
    name: 'Vincent Chan',
    age: 14,
});

```

## Find

Find documents in the database.

```javascript
const collection = db.collection('students');

const result = await collection.find({
    name: 'Vincent Chan',
});

console.log(result);
```

### Advanced Find

PoloDB supports complex find operation like MongoDB.

Example: find all items with age is greater than 18
```javascript
const collection = db.collection('students');

const result = await collection.find({
    age: {
        $gt: 18,
    },
});

```

## Update

Update documents in the database.

```javascript
const collection = db.collection('students');

await collection.update({
    name: 'Vincent Chan',
}, {
    $inc: {
        age: 1,
    },
});
```

## Delete

Delete documents by query.

```javascript
const collection = db.collection('students');

await collection.delete({
    name: 'Vincent Chan',
});

```
