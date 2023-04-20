/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
import { deserialize, serialize, type Document } from "bson";
import { Commands } from "./commands";
import { Database } from "../pkg/polodb_wasm";

export class Collection {

    constructor(private _db: DatabaseDelegate, private _name: string) {}

    insertOne(document: any): Document {
        return this._db._sendMessage({
            command: Commands.Insert,
            ns: this._name,
            documents: [document],
        });
    }

    countDocuments(): number {
        const doc = this._db._sendMessage({
            command: Commands.CountDocuments,
            ns: this._name,
        });
        return doc.count;
    }

}

export class DatabaseDelegate {

    static async open(name: string) {
        const database = new Database();
        await database.open(name);

        return new DatabaseDelegate(database);
    }

    static drop(name: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const req = window.indexedDB.deleteDatabase(name);
            req.onsuccess = () => resolve();
            req.onerror = reject;
        });
    }

    constructor(private _db: Database) {}

    collection(name: string): Collection {
        return new Collection(this, name);
    }

    _sendMessage(doc: Document): Document {
        const buffer = serialize(doc)
        const result = this._db.handleMessage(buffer);
        return deserialize(result);
    }

    close() {
        this._db.free();
    }

}
