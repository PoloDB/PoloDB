const { PoloDbClient, ObjectId } = require('../dist');
const { prepareTestPath } = require('./testUtils');

describe('version', function () {

  test('test version', async () => {
    const version = await PoloDbClient.version();
    expect(version).toBe('PoloDB 2.0.0');
  });

});

describe('Database', function () {
  /**
   * @type {PoloDbClient}
   */
  let client;
  beforeAll(async function() {
    const p = prepareTestPath('test.db');
    client = await PoloDbClient.createConnection(p);
  });

  afterAll(function() {
    if (client) {
      client.dispose();
    }
  });

  test('test serialize', async () => {
    const collection = client.collection('test1');
    const oid = await collection.insert({
      name: 'Vincent Chan',
      gentle: 'man',
    });
    expect(oid).toBeInstanceOf(ObjectId);
    expect(await collection.count()).toBe(1);
    const data = await collection.find();
    console.log(data);
  });

  const TEST_COUNT = 1000;
  test('insert 1000 elements', async () => {
    const collection = client.collection('test2');
    for (let i = 0; i < TEST_COUNT; i++) {
      await collection.insert({
        _id: i,
        hello: i.toString(),
      });
    }
    expect(await collection.count()).toBe(TEST_COUNT);
  });

  test('find 1000 elements', async () => {
    const collection = client.collection('test2');
    for (let i = 0; i < TEST_COUNT; i++) {
      const result = await collection.find({
        _id: i,
      });
      expect(result.length).toBe(1);
      const first = result[0];
      expect(parseInt(first.hello, 10)).toBe(i);
    }
  });

  test('findOne 1000 elements', async () => {
    const collection = client.collection('test2');
    for (let i = 0; i < TEST_COUNT; i++) {
      const result = await collection.findOne({
        _id: i,
      });
      expect(typeof result).toBe('object');
    }
  });

  test('delete 1000 elements', async () => {
    const collection = client.collection('test2');
    for (let i = 0; i < TEST_COUNT; i++) {
      await collection.delete({
        _id: i,
      });
      const result = await collection.find({
        _id: i,
      });
      expect(result.length).toBe(0);
    }
  });

  test('array', async () => {
    const collection = client.collection('test3');
    const arr = [];
    for (let i = 0; i < 1000; i++) {
      arr.push(i);
    }
    await collection.insert({
      data: arr,
    });
    const result = await collection.find();
    expect(result.length).toBe(1);
    const first = result[0];
    expect(Array.isArray(first.data)).toBe(true);
    for (let i = 0; i < 1000; i++) {
      expect(first.data[i]).toBe(i);
    }
  });

  test('datetime', async () => {
    const colDateTime = client.collection('test4');
    const now = new Date();
    await colDateTime.insert({
      created: now,
    });
    const result = await colDateTime.find();
    expect(result.length).toBe(1);
    const first = result[0];
    expect(first.created.getTime()).toBe(now.getTime());
  });

  test('drop collection', async () => {
    await client.dropCollection('test3');
    let thrown = false;
    try {
      const collection = client.collection('test3');
      await collection.find({
        _id: 2
      });
    } catch (err) {
      thrown = true;
    }

  });

});

describe('logic $or and $and', function() {
  /**
   * @type {PoloDbClient}
   */
  let client;
  beforeAll(async function() {
    const p = prepareTestPath('test-update.db');
    client = await PoloDbClient.createConnection(p);
  });

  afterAll(function() {
    if (client) {
      client.dispose();
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

  test('test $or', async () => {
    const collection = client.collection('test');
    for (const item of suite) {
      await collection.insert(item);
    }

    const twoItems = await collection.find({
      $or: [
        {
          age: 11,
        },
        {
          age: 12,
        },
      ]
    });

    expect(twoItems.length).toBe(2);
  });

  test('test $and', async () => {
    const collection = client.collection('test');
    const items = await collection.find({
      $and: [
        {
          name: 'test2',
        },
        {
          age: 11,
        },
      ]
    });

    expect(items.length).toBe(1);
    expect(items[0].name).toBe('test2');
    expect(items[0].age).toBe(11);
  })

});
