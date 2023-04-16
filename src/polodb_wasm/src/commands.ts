/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

export enum Commands {
    Find = "Find",
    Insert = "Insert",
    Update = "Update",
    Delete = "Delete",
    CreateCollection = "CreateCollection",
    DropCollection = "DropCollection",
    CountDocuments = "CountDocuments",
    StartTransaction = "StartTransaction",
    CommitTransaction = "CommitTransaction",
    AbortTransaction = "AbortTransaction",
    StartSession = "StartSession",
    DropSession = "DropSession",
    SafelyQuit = "SafelyQuit",
}
