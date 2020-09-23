const addon = require('bindings')('polodb-js');

console.log(addon.version()); // 'world'

const ObjectIdExt = Symbol("ObjextIdExt");
const ExecSymbol = Symbol("exec");

function compile(obj) {
  new BytecodeBuilder();
  return new ArrayBuffer();
}

const { mkNull, mkDouble } = addon;

class Value {

  static fromRaw(value) {
    let ty = typeof value;
    switch(ty) {
      case "boolean":
        return new Value(addon.mkBool(value));

      case "string":
        return new Value(addon.mkString(value));

      default:
        throw new TypeError("uknown type");

    }
  }

  static makeNull() {
    return new Value(addon.mkNull());
  }

  static makeInt(value) {
    return new Value(addon.mkInt(value));
  }

  static makeDouble(value) {
    return new Value(addon.mkDouble(value));
  }

  constructor(internal) {
    this.__val = internal;
  }

  typeName() {
    return addon.valueTypeName(this.__val);
  }

}

class Document {

  constructor() {
    this.__doc = addon.makeDocument();
  }

  set(key, value) {
    if (!(value instanceof Value)) {
      throw new TypeError("second param should be a DbValue");
    }

    addon.documentSet(this.__doc, key, value.__val);
  }

  get(key) {
    const raw = addon.documentGet(this.__doc, key);
    if (typeof raw === 'undefined') {
      return raw;
    }
    return new Value(raw);
  }

}

class DbArray {

  constructor() {
    this.__arr = addon.mkArray();
  }

  get(index) {
    return addon.arrayGet(this.__arr);
  }

  push(val) {
    if (!(val instanceof Value)) {
      throw new TypeErr("not a Value");
    }
    addon.arrayPush(this.__arr, val.__val);
  }

  length() {
    return addon.arrayLen(this.__arr);
  }

}

class ObjectId {

  constructor(ext) {
    this[ObjectIdExt] = ext;
  }

  toValue() {
    const raw = this[ObjectIdExt];
    const valueRaw = addon.objectIdToValue(raw);
    return new Value(valueRaw);
  }

  hex() {
    const raw = this[ObjectIdExt];
    return addon.objectIdToHex(raw);
  }

}

class Collection {

  constructor(db, name) {
    this.__db = db;
    this.__name = name;
  }

  find(query_obj) {
    const byte_code = compile(query_obj);
    this.__db[ExecSymbol](byte_code);
  }

}

class Database {

  constructor(path) {
    this.__db = addon.open(path);
  }

  [ExecSymbol]() {

  }

  makeObjectId() {
    const raw = addon.mkObjectId(this.__db);
    return new ObjectId(raw);
  }

  createCollection(name) {
    addon.createCollection(this.__db, name);
  }

  collection(name) {
    return new Collection(this, name);
  }

  close() {
    addon.close(this.__db);
  }

}

class BytecodeBuilder {

  constructor() {

  }

  addCommandQuery() {
    console.log('addCommandQuery');
  }

}

module.exports = {
  Database,
  Document,
  DbArray,
  Value,
  mkNull,
  mkDouble,
};
