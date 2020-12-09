
# Node.js binding for PoloDB

## Installation

Npm:

```
npm install --save polodb
```

Yarn:

```
yarn add polodb
```

## Open a database

Remember to close the database.

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

## Create a Collection

Create a collection.

```javascript

db.createCollection('students');

```

## Insert

Insert a document to the database.

```javascript
const collection = db.collection('students');

collection.insert({
    name: 'Vincent Chan',
    age: 14,
});

```

## Find

Find documents in the database.

```javascript
const collection = db.collection('students');

const result = collection.find({
    name: 'Vincent Chan',
});

console.log(result);
```

### Advanced Find

PoloDB supports complex find operation like MongoDB:
[Query Operation](../Query.md)

Example: find all items with age is greater than 18
```javascript
const collection = db.collection('students');

const result = collection.find({
    age: {
        $gt: 18,
    },
});

```

## Update

Update documents in the database: [Update Operation](../Update.md).

```javascript
const collection = db.collection('students');

collection.update({
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

collection.delete({
    name: 'Vincent Chan',
});

```

Delete all documents in the collection.

```javascript
const collection = db.collection('students');
collection.deleteAll();

```
