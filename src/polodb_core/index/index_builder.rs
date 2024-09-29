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

use bson::Document;
use crate::Result;
use crate::coll::collection_info::IndexInfo;
use crate::cursor::Cursor;
use crate::index::{IndexHelper, IndexHelperOperation};
use crate::transaction::TransactionInner;

pub(crate) struct IndexBuilder<'b, 'c, 'd, 'e> {
    txn: &'b TransactionInner,
    col_name: &'c str,
    index_name: &'d str,
    index_info: &'e IndexInfo,
}

impl<'b, 'c, 'd, 'e> IndexBuilder<'b, 'c, 'd, 'e> {

    #[inline]
    pub fn new(
        txn: &'b TransactionInner,
        col_name: &'c str,
        index_name: &'d str,
        index_info: &'e IndexInfo,
    ) -> IndexBuilder<'b, 'c, 'd, 'e> {
        IndexBuilder {
            txn,
            col_name,
            index_name,
            index_info,
        }
    }

    pub fn execute(&mut self, op: IndexHelperOperation) -> Result<()> {
        let multi_cursor = self.txn.rocksdb_txn.new_iterator();
        let mut cursor = Cursor::new_with_str_prefix(
            self.col_name.to_string(),
            multi_cursor,
        )?;

        cursor.reset()?;

        while cursor.has_next() {
            // get the value and insert index
            let current_data = cursor.copy_data()?;

            self.execute_index_item(op, current_data.as_ref())?;

            cursor.next()?;
        }

        Ok(())
    }

    fn execute_index_item(&mut self, op: IndexHelperOperation, current_data: &[u8]) -> Result<()> {
        let data_doc = bson::from_slice::<Document>(current_data)?;
        let pkey = data_doc.get("_id").unwrap();

        IndexHelper::try_execute_with_index_info(
            op,
            &data_doc,
            self.col_name,
            pkey,
            self.index_name,
            self.index_info,
            self.txn,
        )
    }

}
