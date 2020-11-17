const { Database, version } = require("..");
const path = require('path');
const os = require('os');
const fs = require('fs');
const { expect } = require("chai");

let temp;

describe('Transaction', function() {
  let db;

  this.beforeAll(function() {
    if (temp === undefined) {
      temp = os.tmpdir()
      console.log('temp dir: ', temp);
    }
    const dbPath = path.join(temp, 'test-db.db');
    db = new Database(dbPath);
  });

  this.afterAll(function() {
    if (db) {
      db.close();
      db = null;
    }
  });

  it('commit', function() {
    db.startTransaction();
    const collection = db.createCollection('test-trans');
    collection.insert({
      _id: 3,
      name: "2333",
    });
    db.commit();
    const result = collection.find({
      name: "2333",
    });
    expect(result.length).to.be(1);
  });

});
