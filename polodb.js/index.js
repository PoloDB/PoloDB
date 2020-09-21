const addon = require('bindings')('polodb-js');

console.log(addon.version()); // 'world'

const ObjectIdExt = Symbol("ObjextIdExt");
const ExecSymbol = Symbol("exec");

function compile(obj) {
  const builder = new BytecodeBuilder();
  return new ArrayBuffer();
}

class Document {

  constructor() {
    this.__doc = addon.makeDocument();
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
};
