const { Database, DbArray, Document, Value, version } = require("./index");

console.log('version', version());

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
  collection.insert({
    hello: -1,
  });
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


