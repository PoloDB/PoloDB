const { Database } = require("..");
const path = require('path');
const os = require('os');
const { expect } = require("chai");
const fs = require('fs');

let temp;

const DATA_SET = [
];

function generateData() {
  for (let i = 0; i < 1000; i++) {
    DATA_SET.push({
      _id: i,
      content: i.toString(),
    });
  }
}

describe('Update', function () {
  let db;
  let dbPath;

  this.beforeAll(function() {
    generateData();
    if (temp === undefined) {
      temp = os.tmpdir()
      console.log('temp dir: ', temp);
    }
    dbPath = path.join(temp, 'test-update.db');
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
    db = new Database(dbPath);
  });

  this.afterAll(function() {
    if (db) {
      db.close();
      db = null;
    }
  });

  it('insert', function() {
    const collection = db.collection('test');

    DATA_SET.forEach(item => {
      collection.insert(item);
    });
  });

  it('update', function() {
    const collection = db.collection('test');
    collection.update({
      _id: {
        $gte: 500
      },
    }, {
      $set: {
        content: "updated!",
      }
    });

    const result = collection.find({
      content: "updated!",
    });
    expect(result.length).to.equals(500);
    expect(result[0]._id).to.equals(500);
  });

});
