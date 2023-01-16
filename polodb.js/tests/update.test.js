const { PoloDbClient } = require('../dist');
const { prepareTestPath } = require('./testUtils');

const DATA_SET = [];

jest.setTimeout(600000);

function generateData() {
  for (let i = 0; i < 1000; i++) {
    DATA_SET.push({
      _id: i,
      num: i,
      content: i.toString(),
    });
  }
}

describe.only('Update', function () {
  /**
   * @type {PoloDbClient}
   */
  let db;
  let dbPath;

  beforeAll(async function() {
    generateData();
    dbPath = prepareTestPath('test-update.db');
    db = await PoloDbClient.createConnection(dbPath);
  });

  afterAll(function() {
    if (db) {
      db.dispose();
    }
  });

  test('insert', async () => {
    const collection = db.collection('test');

    // for (const item of DATA_SET) {
    //   await collection.insert(item);
    // }
    const promises = DATA_SET.map(item => {
      return collection.insert(item);
    });
    await Promise.all(promises);
  });

  test('update $gte $set', async () => {
    const collection = db.collection('test');
    await collection.update({
      _id: {
        $gte: 500
      },
    }, {
      $set: {
        content: "updated!",
      }
    });

    const result = await collection.find({
      content: "updated!",
    });
    expect(result.length).toBe(500);
    expect(result[0]._id).toBe(500);
  });

  test('throw error whiling updating primary key', async () => {
    const collection = db.collection('test');
    let thrown = false;
    try {
      await collection.update({
        _id: 0
      }, {
        $inc: {
          _id: 100
        },
      });
    } catch (err) {
      thrown = true;
    }
    expect(thrown).toBe(true);
  });

  test('update $inc', async () => {
    const collection = db.collection('test');
    await collection.update({
      _id: 0
    }, {
      $inc: {
        num: 100
      },
    });
    const result = await collection.find({
      _id: 0,
    });
    expect(result.length).toBe(1);
    expect(result[0].num).toBe(100);
  });

  test('update $rename', async () => {
    const collection = db.collection('test');
    await collection.update({
      _id: 0
    }, {
      $rename: {
        num: 'num2'
      },
    });
    const result = await collection.find({
      _id: 0,
    });
    expect(result.length).toBe(1);
    expect(result[0]._id).toBe(0);
    expect(result[0].num).toBe(undefined);
    expect(result[0].num2).toBe(100);
  });

  test('update $unset', async () => {
    const collection = db.collection('test');
    await collection.update({
      _id: 0
    }, {
      $unset: {
        num2: ''
      },
    });
    const result = await collection.find({
      _id: 0,
    });
    expect(result[0]._id).toBe(0);
    expect(result[0].num2).toBe(undefined);
  });

  test('update $max', async () => {
    const collection = db.collection('test');
    await collection.update({
      _id: 1,
    }, {
      $max: {
        num: 0 
      },
    });
    let result = await collection.find({
      _id: 1,
    });
    expect(result[0].num).toBe(1);
    await collection.update({
      _id: 1,
    }, {
      $max: {
        num: 2,
      },
    });
    result = await collection.find({
      _id: 1,
    });
    expect(result[0].num).toBe(2);
  });

  test('update $push', async () => {
    const collection = db.collection('test-push');
    await collection.insert({
      _id: 0,
      content: [ 1, 2, 3 ],
    });
    await collection.update({
      _id: 0,
    }, {
      $push: {
        content: 4,
      },
    });
    const item = await collection.findOne({ _id: 0 });
    expect(item.content.length).toBe(4);
  });

  test.skip('update $pop', async () => {
    const collection = db.collection('test-pop');
    await collection.insert({
      _id: 0,
      content: [ 1, 2, 3 ],
    });
    await collection.update({
      _id: 0,
    }, {
      $pop: {
        content: 1,
      },
    });
    let item = await collection.findOne({ _id: 0 });
    expect(item.content).to.deep.equal([ 1, 2 ]);
    await collection.update({
      _id: 0,
    }, {
      $pop: {
        content: -1,
      }
    });
    item = await collection.findOne({ _id: 0 });
    expect(item.content).to.deep.equal([ 2 ]);
    // expect(function() {
    //   collection.update({
    //     _id: 0,
    //   }, {
    //     $pop: {
    //       content: 'content', 
    //     }
    //   });
    // }).to.throw(Error);
  });

});
