/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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
            &pkey,
            self.index_name,
            self.index_info,
            self.txn,
        )
    }

}
