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

  // for (let i = 0; i < 100; i++) {
  //   collection.insert({
  //     hello: i,
  //   });
  // }

  const count = collection.update({
    hello: 600, 
  }, {
    $min: {
      hello: 400,
    }
  });

  console.log('count:', count);

  const allData = collection.findAll();
  console.log(allData);
} catch(e) {
  console.error(e);
} finally {
  db.close();
}


