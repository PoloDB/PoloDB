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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;
use bson::{Document, RawDocumentBuf};
use polodb_core::{ClientCursor, Database};
use crate::handlers::Handler;
use anyhow::Result;

#[derive(Clone)]
pub(crate) struct AppContext {
    inner: Arc<AppContextInner>,
}

impl AppContext {

    pub(crate) fn new(db: Database) -> Self {
        AppContext {
            inner: Arc::new(AppContextInner::new(db)),
        }
    }

    #[inline]
    pub(crate) fn db(&self) -> Arc<Database> {
        self.inner.db.clone()
    }

    pub(crate) fn push_handler(&self, handler: Arc<dyn Handler>) {
         let mut handlers = self.inner.handlers.lock().unwrap();
         handlers.push(handler);
     }

    pub(crate) fn get_handlers(&self, doc: &RawDocumentBuf) -> Result<Option<Arc<dyn Handler>>> {
        let handlers = self.inner.handlers.lock().unwrap();
        for handler in handlers.iter() {
            let test = handler.test(doc)?;
            if test {
                return Ok(Some(handler.clone()));
            }
        }
        Ok(None)
    }

    pub(crate) fn next_conn_id(&self) -> u64 {
        self.inner.conn_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub (crate) fn save_cursor(&self, cursor: Arc<Mutex<ClientCursor<Document>>>) -> i64 {
        let mut cursors = self.inner.cursors.lock().unwrap();
        let mut cursor_id = cursors.len() as i64;
        cursor_id += 1; // cursor_id starts from 1
        cursors.insert(cursor_id, cursor);
        cursor_id
    }

    pub(crate) fn get_cursor(&self, cursor_id: i64) -> Option<Arc<Mutex<ClientCursor<Document>>>> {
        let cursors = self.inner.cursors.lock().unwrap();
        cursors.get(&cursor_id).map(|c| c.clone())
    }

    pub(crate) fn remove_cursor(&self, cursor_ids: &[i64]) {
        let mut cursors = self.inner.cursors.lock().unwrap();
        for cursor_id in cursor_ids {
            cursors.remove(cursor_id);
        }
    }
}

struct AppContextInner {
    db: Arc<Database>,
    handlers: Mutex<Vec<Arc<dyn Handler>>>,
    cursors: Mutex<HashMap<i64, Arc<Mutex<ClientCursor<Document>>>>>,
    conn_id: AtomicU64,
}

impl AppContextInner {

    fn new(db: Database) -> Self {
        AppContextInner {
            db: Arc::new(db),
            handlers: Mutex::new(Vec::with_capacity(32)),
            cursors: Mutex::new(HashMap::new()),
            conn_id: AtomicU64::new(0),
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn db(&self) -> Arc<Database> {
        self.db.clone()
    }

}

impl Drop for AppContextInner {

    fn drop(&mut self) {
        let mut cursors = self.cursors.lock().unwrap();
        cursors.clear();
    }

}
