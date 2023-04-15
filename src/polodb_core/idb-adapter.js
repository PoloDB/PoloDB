/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

const STORE_NAME_METAS = "metas";
const STORE_NAME_SEGMENTS = "segments";
const STORE_NAME_LOGS = "logs";

/**
 *
 * @param {string} name
 * @returns {Promise<IDBDatabase>}
 */
async function open_db(name) {
    return new Promise((resolve, reject) => {
        let open_db_req = window.indexedDB.open(name);
        open_db_req.onupgradeneeded = () => {
            let db = open_db_req.result;

            db.createObjectStore(STORE_NAME_METAS, {
                keyPath: "id",
            });
            db.createObjectStore(STORE_NAME_SEGMENTS, {
                keyPath: "id",
            });
            const log_store = db.createObjectStore(STORE_NAME_LOGS, {
                autoIncrement: true,
            });
            log_store.createIndex("session", "session");
        }
        open_db_req.onsuccess = () => {
            resolve(open_db_req.result);
        }
        open_db_req.onerror = reject;
    });
}

/**
 *
 * @param {IDBObjectStore} metas_store
 * @returns {Promise<any>}
 */
async function read_latest_meta(metas_store) {
    return new Promise((resolve, reject) => {
        let cursor_req = metas_store.openCursor(null,  "prev");
        cursor_req.onsuccess = (ev) => {
            const cursor = cursor_req.result;
            resolve(cursor?.value);
        }

        cursor_req.onerror = reject;
    });
}

/**
 *
 * @param {IDBTransaction} transaction
 * @param {Map<string, any>} map
 * @returns {Promise<any>}
 */
function read_segment(transaction, map, segment) {
    return new Promise((resolve, reject) => {
        const segments_store = transaction.objectStore(STORE_NAME_SEGMENTS);

        const cursor = segments_store.openCursor(segment);

        cursor.onsuccess = (e) => {
            resolve(cursor.result);
        }
        cursor.onerror = reject;
    });
}

/**
 *
 * @param {IDBTransaction} transaction
 * @param {string} session_id
 * @returns {Promise<any[]>}
 */
function read_logs_by_session_id(transaction, session_id) {
    return new Promise((resolve, reject) => {
        const logs_store = transaction.objectStore(STORE_NAME_LOGS);

        const session_index = logs_store.index("session");

        const cursor_req = session_index.openCursor(IDBKeyRange.only(session_id));
        const result = [];

        cursor_req.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
                result.push(cursor.value);

                cursor.continue();
            } else {
                resolve(result);
            }
        }

        cursor_req.onerror = reject;
    });
}

/**
 *
 * @param {string} name
 * @returns {Promise<any>}
 */
export async function load_snapshot(name) {
    let db = await open_db(name);

    const transaction = db.transaction([
        STORE_NAME_METAS,
        STORE_NAME_SEGMENTS,
        STORE_NAME_LOGS,
    ], "readonly");

    const metas_store = transaction.objectStore(STORE_NAME_METAS);
    const latest_meta = await read_latest_meta(metas_store);
    // no latest meta
    if (!latest_meta) {
        return {
            db,
        };
    }

    const data = new Map();

    for (const level of latest_meta.levels) {
        for (const segment of level.segments) {
            const item = await read_segment(transaction, map, segment);
            if (item) {
                data.set(segment, item);
            }
        }
    }

    const session_id = latest_meta.session_id;

    const logs_data = await read_logs_by_session_id(transaction, session_id);

    return {
        db,
        session_id,
        snapshot: latest_meta,
        segments: data,
        logs_data,
    }
}

export class IdbBackendAdapter {

    /**
     *
     * @param {IDBDatabase} db
     */
    constructor(db) {
        this._db = db;
    }

    write_snapshot_to_idb(snapshot) {
        return new Promise((resolve, reject) => {
            const transaction = this._db.transaction([STORE_NAME_METAS], "readwrite");

            const metas_store = transaction.objectStore(STORE_NAME_METAS);

            const req = metas_store.put(snapshot);

            transaction.commit();

            req.onsuccess = resolve;
            req.onerror = reject;
        });
    }

    write_segments_to_idb(segments) {
        return new Promise((resolve, reject) => {
            const transaction = this._db.transaction([STORE_NAME_SEGMENTS], "readwrite");

            const segments_store = transaction.objectStore(STORE_NAME_SEGMENTS);

            const req = segments_store.put(segments);

            transaction.commit();

            req.onsuccess = resolve;
            req.onerror = reject;
        });
    }

    dispose() {
        this._db.close();
    }

}

export class IdbLogAdapter {
    /**
     *
     * @param {IDBDatabase} db
     */
    constructor(db) {
        this._db = db;
    }

    commit(buffer) {
        const transaction = this._db.transaction([STORE_NAME_LOGS], "readwrite");
        const logs_store = transaction.objectStore(STORE_NAME_LOGS);
        logs_store.put(buffer);
        transaction.commit();
    }

    shrink(session) {
        const transaction = this._db.transaction([STORE_NAME_LOGS], "readwrite");
        const logs_store = transaction.objectStore(STORE_NAME_LOGS);
        const session_index = logs_store.index("session");
        const cursor_req = session_index.openCursor(session);

        cursor_req.onsuccess = (e) => {
            const cursor = cursor_req.result;

            if (cursor.value) {
                cursor.delete();
                cursor.continue()
            }
        }

    }

}
