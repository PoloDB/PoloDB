// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bson::{doc, Document, Regex};

mod common;

use common::prepare_db;

#[test]
fn test_regex() {
    vec![
        (prepare_db("test-regex").unwrap(), true),
    ]
    .iter()
    .for_each(|(db, _)| {
        let metrics = db.metrics();
        metrics.enable();

        let collection = db.collection::<Document>("config");
        let docs = vec![
            doc! {
                "_id": "c1",
                "value": "c1",
            },
            doc! {
                "_id": "invalid",
                "value": "not-valid-value",
            },
            doc! {
                "_id": "c3",
                "value": "c3"
            },
        ];
        collection.insert_many(&docs).unwrap();

        let res = collection
            .find(doc! {
                "value": {
                    "$regex": Regex {
                        pattern: "c[0-9]+".into(),
                        options: "i".into(),
                    },
                }
            })
            .unwrap();

        assert_eq!(res.count(), docs.len() - 1);
    });
}

#[test]
fn test_regex_error() {
    vec![
        (prepare_db("test-regex-error").unwrap(), true),
    ]
    .iter()
    .for_each(|(db, _)| {
        let metrics = db.metrics();
        metrics.enable();

        let collection = db.collection::<Document>("config");
        let docs = vec![
            doc! {
                "_id": "c1",
                "value": "c1",
            },
            doc! {
                "_id": "invalid",
                "value": "not-valid-value",
            },
            doc! {
                "_id": "c3",
                "value": "c3"
            },
        ];
        collection.insert_many(&docs).unwrap();

        let mut res = collection
            .find(doc! {
                "value": {
                    "$regex": Regex {
                        pattern: "c[0-9]+".into(),
                        options: "pml".into(), // invalid option
                    },
                }
            })
            .unwrap();

        assert!(res.next().unwrap().is_err());
    });
}
