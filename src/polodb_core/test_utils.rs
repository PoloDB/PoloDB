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

use std::path::PathBuf;
use std::env;
use crate::{Config, Database, Result};

pub fn mk_db_path(db_name: &str) -> PathBuf {
    let mut db_path = env::temp_dir();
    let db_filename = String::from(db_name) + "-db";
    db_path.push(db_filename);
    let _ = std::fs::remove_dir_all(db_path.as_path());
    db_path
}


pub fn prepare_db_with_config(db_name: &str, config: Config) -> Result<Database> {
    let db_path = mk_db_path(db_name);

    let _ = std::fs::remove_dir_all(db_path.as_path());

    Database::open_path_with_config(db_path.as_path().to_str().unwrap(), config)
}

pub fn prepare_db(db_name: &str) -> Result<Database> {
    prepare_db_with_config(db_name, Config::default())
}