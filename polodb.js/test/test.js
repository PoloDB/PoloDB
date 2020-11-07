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
      db.close();
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
      db.close();
    });

    it('create collection', function() {
      db.createCollection('test-1');
      db.createCollection('test-2');
      db.createCollection('test-3');
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
    })

  });

});
