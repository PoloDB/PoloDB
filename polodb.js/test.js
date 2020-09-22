const { Database, Document, Value } = require("./index");

const three = Value.makeInt(3);

const doc = new Document();
doc.set("hello", three);
doc.set("hello", Value.fromRaw("hello"));
console.log(doc);

// const db = new Database("/tmp/test-node-2");
// db.createCollection("name");

// db.close();

