const { Database, Document } = require("./index");

const doc = new Document();
console.log(doc);

const db = new Database("/tmp/test-node-2");
db.createCollection("name");

db.close();

