const { Database, version } = require("..");
const path = require('path');
const os = require('os');
const { expect } = require("chai");
const fs = require('fs');

let temp;

describe('Transaction', function() {
  let db;
  let dbPath;

  this.beforeAll(function() {
    if (temp === undefined) {
      temp = os.tmpdir()
      console.log('temp dir: ', temp);
    }
    dbPath = path.join(temp, 'test-db.db');
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

  it('commit', function() {
    db.startTransaction();
    let collection = db.createCollection('test-trans');
    expect(collection).to.not.be.undefined;
    collection.insert({
      _id: 3,
      name: "2333",
    });
    db.commit();
    db.close();

    db = new Database(dbPath);
    collection = db.collection('test-trans');
    const result = collection.find({
      name: "2333",
    });
    expect(result.length).to.equals(1);
  });

  it('rollback', function() {
    db.createCollection('test-trans');
    db.startTransaction();
    const collection = db.collection('test-trans');
    let result;
    result = collection.find({
      name: "rollback",
    })
    expect(result.length).to.equals(0);
    collection.insert({
      _id: 4,
      name: "rollback",
    });
    result = collection.find({
      name: "rollback",
    })
    expect(result.length).to.equals(1);
    db.rollback();
    result = collection.find({
      name: "rollback",
    });
    expect(result.length).to.equals(0);
  });

});

describe('abandon uncommited changes', function() {
  let db;
  let dbPath;

  this.beforeAll(function() {
    if (temp === undefined) {
      temp = os.tmpdir()
      console.log('temp dir: ', temp);
    }
    dbPath = path.join(temp, 'test-uncommit.db');
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

  it('run', function() {
    let collection = db.createCollection('test');

    for (let i = 0; i < 10; i++) {
      collection.insert({
        _id: i,
        hello: 'world',
      });
    }

    expect(collection.count()).to.equals(10);

    db.startTransaction();

    for (let i = 10; i < 20; i++) {
      collection.insert({
        _id: i,
        hello: 'world',
      });
    }

    db.close();

    db = new Database(dbPath);

    collection = db.collection('test');
    expect(collection.count()).to.equals(10);
  });

});
