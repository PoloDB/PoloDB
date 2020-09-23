const { Database, DbArray, Document, Value } = require("./index");

const three = Value.makeInt(3);
console.log(three.typeName());

const doc = new Document();
doc.set("hello", three);
doc.set("hello", Value.fromRaw("hello"));
console.log(doc);

const arr = new DbArray();
arr.push(three);
console.log('len:', arr.length());

const db = new Database("/tmp/test-node-3");

const oid = db.makeObjectId();
console.log(oid.hex());

// db.createCollection("name");

db.close();

