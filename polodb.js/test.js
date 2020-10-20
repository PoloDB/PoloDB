const { Database, DbArray, Document, Value, version, UTCDatetime } = require("./index");

const time = UTCDatetime.fromTimestamp(1000);
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

  // for (let i = 0; i < 10; i++) {
  //   collection.insert({
  //     hello: i,
  //   });
  // }

  // const count = collection.update({
  //   hello: 600, 
  // }, {
  //   $min: {
  //     hello: 400,
  //   }
  // });

  // console.log('count:', count);

  // db.createCollection('test_col2');
  // const collection = db.collection("test_col");

  // for (let i = 0; i < 10; i++) {
  //   collection.insert({
  //     _id: i,
  //     content: i.toString(),
  //   });
  // }

  const allData = collection.findAll();

  const first = allData[2];

  allData.forEach((item, index) => {
    console.log('id: ', index, ', ', item._id.toString());
    console.log(item);
  });

  const firstId = first['_id'];

  console.log('firstId', firstId);
  console.log('first hex: ', firstId.toString());
  const result = collection.find({
    _id: firstId,
    // hello: 3
  });

  console.log(result);
  // console.log(result[0].toString());
} catch(e) {
  console.error(e);
} finally {
  db.close();
}


