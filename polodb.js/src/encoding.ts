import * as BSON from 'bson';

export function decode(buffer: Uint8Array) {
  return BSON.deserialize(buffer)
}

export function encode(obj: unknown) {
  return BSON.serialize(obj);
}
