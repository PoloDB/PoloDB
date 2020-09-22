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

}

class ObjectId {

  constructor(ext) {
    this[ObjectIdExt] = ext;
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
  Value,
  mkNull,
  mkDouble,
};
