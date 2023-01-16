const { PoloDbClient } = require('../dist');
const { prepareTestPath } = require('./testUtils');

describe('transaction', function() {
  /**
   * @type {PoloDbClient}
   */
  let client;
  let p;
  beforeAll(async function() {
    p = prepareTestPath('test-transaction.db');
    client = await PoloDbClient.createConnection(p);
  });

  afterAll(function() {
    if (client) {
      client.dispose();
    }
  });

  test('test serialize', async () => {
    await client.startTransaction();
    let collection = client.collection('test-trans');
    await collection.insert({
      _id: 3,
      name: "2333",
    });
    await client.commit();
    client.dispose();

    client = await PoloDbClient.createConnection(p);
    collection = client.collection('test-trans');
    const result = await collection.find({
      name: "2333",
    });
    expect(result.length).toBe(1);
  });

  test('rollback', async () => {
    const collection = await client.createCollection('test-trans-2');
    await client.startTransaction();
    let result;
    result = await collection.find({
      name: "rollback",
    })
    expect(result.length).toBe(0);
    await collection.insert({
      _id: 4,
      name: "rollback",
    });
    result = await collection.find({
      name: "rollback",
    });
    expect(result.length).toBe(1);
    await client.rollback();
    result = await collection.find({
      name: "rollback",
    });
    expect(result.length).toBe(0);
  });

});

describe('abandon uncommited changes', function() {
  let db;
  let dbPath;

  beforeAll(async function() {
    dbPath = prepareTestPath('test-transaction.db');
    db = await PoloDbClient.createConnection(dbPath);
  });

  afterAll(function() {
    if (db) {
      db.dispose();
    }
  });

  test('run', async () => {
    let collection = await db.createCollection('test');

    for (let i = 0; i < 10; i++) {
      await collection.insert({
        _id: i,
        hello: 'world',
      });
    }

    expect(await collection.count()).toBe(10);

    await db.startTransaction();

    for (let i = 10; i < 20; i++) {
      await collection.insert({
        _id: i,
        hello: 'world',
      });
    }

    db.dispose();

    db = await PoloDbClient.createConnection(dbPath);

    collection = db.collection('test');
    expect(await collection.count()).toBe(10);
  });

});
