const { Database, version } = require("..");
const path = require('path');
const os = require('os');
const fs = require('fs');
const { expect } = require("chai");

let temp;

describe('Database', function() {
  this.beforeAll(function () {
    if (temp === undefined) {
      temp = os.tmpdir()
      console.log('temp dir: ', temp);
    }
  });

  describe('open', function() {
    let db;
    this.beforeAll(function() {
      const dbPath = path.join(temp, 'test-db.db');
      db = new Database(dbPath);
    });

    this.afterAll(function() {
      if (db) {
        db.close();
        db = null;
      }
    });

    it('print version', function() {
      const dbVersion = version();
      expect(typeof dbVersion).equals('string');
    })

  });

  describe('collection', function() {
    let db;
    this.beforeAll(function() {
      const dbPath = path.join(temp, 'test-collection.db');
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
      }
      const journalPath = dbPath + '.journal';
      if (fs.existsSync(journalPath)) {
        fs.unlinkSync(journalPath);
      }
      db = new Database(dbPath);
    });

    this.afterAll(function() {
      if (db) {
        db.close();
      }
    });

    it('create collection', function() {
      db.createCollection('test-1');
      db.createCollection('test-2');
      db.createCollection('test-3');
    });

    it('test auto id', function() {
      const col1 = db.collection('test-1');
      const insertObj = {
        name: 'Vincent Chan',
      };
      col1.insert(insertObj);
      expect('_id' in insertObj).to.be.true;
      const objIdHex = insertObj['_id'].toString();
      expect(objIdHex.length).to.equals(16);
    });

    const TEST_COUNT = 1000;
    it('insert', function() {
      const col2 = db.collection('test-2');
      for (let i = 0; i < TEST_COUNT; i++) {
        col2.insert({
          _id: i,
          hello: i.toString(),
        });
      }
      expect(col2.count()).to.equals(TEST_COUNT);
    });

    it('test array', function() {
      const colArray = db.createCollection('test-array');
      const arr = [];
      for (let i = 0; i < 1000; i++) {
        arr.push(i);
      }
      colArray.insert({
        data: arr,
      });
      const result = colArray.find();
      expect(result.length).to.equals(1);
      const first = result[0];
      expect(Array.isArray(first.data)).to.be.true;
      for (let i = 0; i < 1000; i++) {
        expect(first.data[i]).to.equals(i);
      }
    });

    it('test datetime', function() {
      const colDateTime = db.createCollection('test-datetime');
      const now = new Date();
      colDateTime.insert({
        created: now,
      });
      const result = colDateTime.find();
      expect(result.length).to.equals(1);
      const first = result[0];
      expect(first.created.getTime()).to.equals(now.getTime());
    });

    it('count', function() {
      const col2 = db.collection('test-2');
      const count = col2.count();
      expect(count).eq(TEST_COUNT);
    });

    it('find', function() {
      const col2 = db.collection('test-2');
      for (let i = 0; i < TEST_COUNT; i++) {
        const result = col2.find({
          _id: i,
        });
        expect(result.length, 1);
        const first = result[0];
        expect(parseInt(first.hello, 10)).eq(i);
      }
    });

    it('findOne()', function() {
      const col2 = db.collection('test-2');
      const one = col2.findOne({
        _id: 1,
      });

      // expect(typeof one).to.equals('object');
      // expect(one._id).to.equals(1);
      console.log('finished');
    });

    it('delete', function() {
      const col2 = db.collection('test-2');
      for (let i = 0; i < TEST_COUNT; i++) {
        col2.delete({
          _id: i,
        });
        const result = col2.find({
          _id: i,
        });
        expect(result.length, 0);
      }
    });

    it('auto generate collection', function() {
      const test = db.collection('auto-gen');
      test.insert({
        content: 'name',
      });
    })

    it('drop', function() {
      const col2 = db.collection('test-2');
      col2.drop();
      expect(() => {
        col2.find({
          _id: 3,
        })
      }).to.throw(Error);
    })

    it('use collection after close', function() {
      const col2 = db.collection('test-3');
      db.close();
      db = null;
      expect(() => {
        col2.find({
          _id: 2,
        });
      }).to.throw(Error);
    })

  });

});

describe('create collection with same name', function() {

    let db;
    this.beforeAll(function() {
      const dbPath = path.join(temp, 'test-same-name.db');
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
      }
      const journalPath = dbPath + '.journal';
      if (fs.existsSync(journalPath)) {
        fs.unlinkSync(journalPath);
      }
      db = new Database(dbPath);
    });

    this.afterAll(function() {
      if (db) {
        db.close();
      }
    });

    it('test', function() {
      db.createCollection('test');
      expect(function() {
        db.createCollection('test');
      }).to.throw(Error);

    });

});

describe('logic $or and $and', function() {

  let db;
  this.beforeAll(function() {
    const dbPath = path.join(temp, 'test-same-name.db');
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
    const journalPath = dbPath + '.journal';
    if (fs.existsSync(journalPath)) {
      fs.unlinkSync(journalPath);
    }
    db = new Database(dbPath);
  });

  this.afterAll(function() {
    if (db) {
      db.close();
    }
  });

  const suite = [
    {
      name: 'test1',
      age: 10,
    },
    {
      name: 'test2',
      age: 11,
    },
    {
      name: 'test3',
      age: 12,
    },
    {
      name: 'test3',
      age: 14,
    }
  ]

  it('test $or', function() {
    const collection = db.createCollection('test');
    for (const item of suite) {
      collection.insert(item);
    }

    const twoItems = collection.find({
      $or: [
        {
          age: 11,
        },
        {
          age: 12,
        },
      ]
    });

    expect(twoItems.length).to.equals(2);
  })

  it('test $and', function () {
    const collection = db.collection('test');
    const items = collection.find({
      $and: [
        {
          name: 'test2',
        },
        {
          age: 11,
        },
      ]
    });

    expect(items.length).to.equals(1);
    expect(items[0].name).to.equals('test2');
    expect(items[0].age).to.equals(11);
  })

});
