const { Database } = require("..");
const path = require('path');
const os = require('os');
const { expect } = require("chai");
const fs = require('fs');

let temp;

const DATA_SET = [];

function generateData() {
  for (let i = 0; i < 1000; i++) {
    DATA_SET.push({
      _id: i,
      num: i,
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

  it('update $gte $set', function() {
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

  it('throw error whiling updating primary key', function() {
    const collection = db.collection('test');
    expect(function () {
      collection.update({
        _id: 0
      }, {
        $inc: {
          _id: 100
        },
      });
    }).to.throw(Error);
  });

  it('update $inc', function () {
    const collection = db.collection('test');
    collection.update({
      _id: 0
    }, {
      $inc: {
        num: 100
      },
    });
    const result = collection.find({
      _id: 0,
    });
    expect(result.length).to.equals(1);
    expect(result[0].num).to.equals(100);
  });

  it('update $rename', function () {
    const collection = db.collection('test');
    collection.update({
      _id: 0
    }, {
      $rename: {
        num: 'num2'
      },
    });
    const result = collection.find({
      _id: 0,
    });
    expect(result.length).to.equals(1);
    expect(result[0]._id).to.equals(0);
    expect(result[0].num).to.be.undefined;
    expect(result[0].num2).to.equals(100);
  });

  it('update $unset', function() {
    const collection = db.collection('test');
    collection.update({
      _id: 0
    }, {
      $unset: {
        num2: ''
      },
    });
    const result = collection.find({
      _id: 0,
    });
    expect(result[0]._id).to.equals(0);
    expect(result[0].num2).to.be.undefined;
  });

  it('update $max', function() {
    const collection = db.collection('test');
    collection.update({
      _id: 1,
    }, {
      $max: {
        num: 0 
      },
    });
    let result = collection.find({
      _id: 1,
    });
    expect(result[0].num).to.equals(1);
    collection.update({
      _id: 1,
    }, {
      $max: {
        num: 2,
      },
    });
    result = collection.find({
      _id: 1,
    });
    expect(result[0].num).to.equals(2);
  });

});
