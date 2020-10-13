const addon = require('bindings')('polodb-js');
const { typeName } = require('./typeName');

function version() {
  return addon.version();
}

const NativeExt = Symbol("NativeExt");

const DB_HANDLE_STATE_HAS_ROW = 2;

class Value {

  static fromRaw(value) {
    let ty = typeof value;
    switch(ty) {
      case "number":
        if (Number.isInteger(value)) {
          return Value.makeInt(value);
        } else {
          return value.makeDouble(value);
        }

      case "boolean":
        return new Value(addon.mkBool(value));

      case "string":
        return new Value(addon.mkString(value));

      case "object":
        if (Array.isArray(value)) {
          return DbArray.fromRaw(value);
        }
        if (value instanceof ObjectId) {
          return value.toValue();
        }
        return Document.fromRaw(value).toValue();

      default:
        throw new TypeError("uknown type: " + ty);

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

  asNumber() {
    if (this.typeName() === "Int") {
      return addon.valueGetNumber(this[NativeExt]);
    }
    return addon.valueGetDouble(this[NativeExt]);
  }

  asBool() {
    return addon.valueGetBool(this[NativeExt]);
  }

  asString() {
    return addon.valueGetString(this[NativeExt]);
  }

  asArray() {
    const raw = addon.valueGetArray(this[NativeExt]);
    return new DbArray(raw);
  }

  asDocument() {
    const doc = addon.valueGetDocument(this[NativeExt]);
    return new Document(doc);
  }

  asObjectId() {
    const raw = addon.valueGetObjectId(this[NativeExt]);
    return new ObjectId(raw);
  }

  typeName() {
    const tyInt = addon.valueType(this[NativeExt]);
    return typeName(tyInt);
  }

  toJsObject() {
    switch (this.typeName()) {
      case "Null":
        return null;

      case "Dobule":
      case "Int":
        return this.asNumber();

      case "Boolean":
        return this.asBool();

      case "String":
        return this.asString();

      case "ObjectId":
        return this.asObjectId();

      case "Array":
        return this.asArray();

      case "Document": {
        let doc = this.asDocument();
        return doc.toJsObject();
      }

      default:
        return undefined;

    }
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

    for (const key in doc) {
      const jsValue = doc[key];
      const dbValue = Value.fromRaw(jsValue);
      result.set(key, dbValue);
    }

    return result;
  }

  constructor(ext) {
    if (typeof ext === 'undefined') {
      this[NativeExt] = addon.makeDocument();
    } else {
      this[NativeExt] = ext;
    }
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

  iter() {
    const rawIter = addon.mkDocIter(this[NativeExt]);
    return new DocumentIter(rawIter);
  }

  toJsObject() {
    const result = {};
    const iterator = this.iter();
    let next = iterator.next();

    while (typeof next !== 'undefined') {
      const [key, value] = next;

      result[key] = value.toJsObject();

      next = iterator.next();
    }

    return result;
  }

  toValue() {
    return new Value(addon.docToValue(this[NativeExt]));
  }

  get length() {
    const len = addon.documentLen(this[NativeExt]);
    return len;
  }

}

class DocumentIter {

  constructor(ext) {
    this[NativeExt] = ext;
  }

  next() {
    const result = addon.docIterNext(this[NativeExt]);
    if (typeof result === 'undefined') {
      return undefined;
    }
    const [key, raw] = result;
    return [key, new Value(raw)];
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

  constructor(ext) {
    if (typeof ext === 'undefined') {
      this[NativeExt] = addon.mkArray();
    } else {
      this[NativeExt] = ext;
    }
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

  get length() {
    return addon.arrayLen(this[NativeExt]);
  }

}

class ObjectId {

  constructor(ext) {
    this[NativeExt] = ext;
  }

  toValue() {
    const raw = this[NativeExt];
    const valueRaw = addon.objectIdToValue(raw);
    return new Value(valueRaw);
  }

  hex() {
    const raw = this[NativeExt];
    return addon.objectIdToHex(raw);
  }

  toString() {
    return this.hex();
  }

}

class Collection {

  constructor(db, name) {
    this.__db = db;
    this.__name = name;
  }

  findAll() {
    return this.find(null);
  }

  insert(data) {
    if (!(data instanceof Document)) {
      data = Document.fromRaw(data);
    }
    return addon.dbInsert(this.__db[NativeExt], this.__name, data[NativeExt]);
  }

  delete(query) {
    if (typeof query === 'undefined') {
      throw new TypeError("query param is missing");
    } else if (!(query instanceof Document)) {
      query = Document.fromRaw(query);
    }
    return addon.dbDelete(this.__db[NativeExt], this.__name, query[NativeExt]);
  }

  deleteAll() {
    return addon.dbDeleteAll(this.__db[NativeExt], this.__name);
  }

  update(query, update) {
    if (typeof query !== 'undefined' && !(query instanceof Document)) {
      query = Document.fromRaw(query);
    }

    if (!(update instanceof Document)) {
      update = Document.fromRaw(update);
    }

    return addon.dbUpdate(
      this.__db[NativeExt],
      this.__name,
      query[NativeExt],
      update[NativeExt]
    );
  }

  find(queryObj) {
    let nativeExt = null;
    if (queryObj instanceof Document) {
      nativeExt = queryObj[NativeExt];
    } else if (typeof queryObj === 'object') {
      const queryDoc = Document.fromRaw(queryObj);
      nativeExt = queryDoc[NativeExt];
    } else if (nativeExt !== null && typeof nativeExt !== 'undefined') {
      throw new TypeError("illegal param");
    }

    const handleRaw = addon.dbFind(this.__db[NativeExt], this.__name, nativeExt);
    const handle = new DbHandle(handleRaw);
    handle.step();

    const result = [];
    while (handle.hasRow()) {
      const value = handle.get();
      result.push(value.toJsObject());

      handle.step();
    }

    return result;
  }

}

class DbHandle {

  constructor(ext) {
    this[NativeExt] = ext;
  }

  step() {
    addon.dbHandleStep(this[NativeExt]);
  }

  state() {
    return addon.dbHandleState(this[NativeExt]);
  }

  get() {
    const rawValue = addon.dbHandleGet(this[NativeExt]);
    return new Value(rawValue);
  }

  hasRow() {
    return this.state() == DB_HANDLE_STATE_HAS_ROW;
  }

  toString() {
    return addon.dbHandleToStr(this[NativeExt]);
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

  startTransaction() {
    addon.startTransaction(this[NativeExt], 0);
  }

  rollback() {
    addon.rollback(this[NativeExt]);
  }

  commit() {
    addon.commit(this[NativeExt]);
  }

}

module.exports = {
  Database,
  Document,
  DbArray,
  Value,
  version,
};
