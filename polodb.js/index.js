const addon = require('bindings')('polodb-js');

console.log(addon.version()); // 'world'

const NativeExt = Symbol("NativeExt");

const { mkNull, mkDouble } = addon;

class Value {

  static fromRaw(value) {
    let ty = typeof value;
    switch(ty) {
      case "boolean":
        return new Value(addon.mkBool(value));

      case "string":
        return new Value(addon.mkString(value));

      case "object":
        if (Array.isArray(value)) {
          return DbArray.fromRaw(value);
        }
        return new Document.fromRaw(value);

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
    this[NativeExt] = internal;
  }

  typeName() {
    return addon.valueTypeName(this[NativeExt]);
  }

}

/**
 * equivalent to Object in JavaScript
 */
class Document {

  /**
   * TODO: check cyclic references
   * @param {Object} doc 
   */
  static fromRaw(doc) {
    const result = new Document();

    for (key in doc) {
      const jsValue = doc[key];
      const dbValue = Value.fromRaw(jsValue);
      result.set(key, dbValue);
    }

    return result;
  }

  constructor() {
    this[NativeExt] = addon.makeDocument();
  }

  set(key, value) {
    if (!(value instanceof Value)) {
      throw new TypeError("second param should be a DbValue");
    }

    addon.documentSet(this[NativeExt], key, value[NativeExt]);
  }

  get(key) {
    const raw = addon.documentGet(this[NativeExt], key);
    if (typeof raw === 'undefined') {
      return raw;
    }
    return new Value(raw);
  }

}

class DbArray {

  /**
   * 
   * @param {Array} arr 
   */
  static fromRaw(arr) {
    if (!Array.isArray(arr)) {
      throw new TypeError("Object must be an array");
    }

    const result = new DbArray();

    for (const elm in arr) {
      const dbElm = Value.fromRaw(elm);
      result.push(dbElm);
    }

    return result;
  }

  constructor() {
    this[NativeExt] = addon.mkArray();
  }

  get(index) {
    return addon.arrayGet(this[NativeExt], index);
  }

  push(val) {
    if (!(val instanceof Value)) {
      throw new TypeErr("not a Value");
    }
    addon.arrayPush(this[NativeExt], val[NativeExt]);
  }

  length() {
    return addon.arrayLen(this[NativeExt]);
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
    console.log(query_obj);
  }

}

class Database {

  constructor(path) {
    this[NativeExt] = addon.open(path);
  }

  makeObjectId() {
    const raw = addon.mkObjectId(this[NativeExt]);
    return new ObjectId(raw);
  }

  createCollection(name) {
    addon.createCollection(this[NativeExt], name);
  }

  collection(name) {
    return new Collection(this, name);
  }

  close() {
    addon.close(this[NativeExt]);
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
