const { Database, DbArray, Document, Value, version } = require("./index");

console.log('version', version());

const three = Value.makeInt(3);
console.log(three.typeName(), three.asNumber());

const str = Value.fromRaw("haha string");
console.log(str.asString());

const doc = new Document();
doc.set("hello", three);
doc.set("hello2", Value.fromRaw("hello"));
console.log(doc.toJsObject());

const arr = new DbArray();
arr.push(three);
console.log('len:', arr.length);

const db = new Database("/tmp/test-node-3");

const oid = db.makeObjectId();
console.log(oid.hex());

// try {
//   db.createCollection("test_col");
// } catch (err) {
//   console.error(err);
// }

try {
  const collection = db.collection("test_col");
  // collection.insert(doc);
  const allData = collection.findAll();
  console.log(allData);

  const age = collection.find({
    hello: 3,
  });
  console.log('age');
  console.log(age);
} catch(e) {
  console.error(e);
} finally {
  db.close();
}


