/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use bson::Document;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexModel {
    #[serde(rename = "key")]
    pub keys: Document,

    /// The options for the index.
    #[serde(flatten)]
    pub options: Option<IndexOptions>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexOptions {

    /// Specifies a name outside the default generated name.
    pub name: Option<String>,

    /// Forces the index to be unique so the collection will not accept documents where the index
    /// key value matches an existing value in the index. The default value is false.
    pub unique: Option<bool>,

}
