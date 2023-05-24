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
use crate::LsmKv;
use crate::session::SessionInner;

pub(crate) struct IndexBuilder<'a, 'b, 'c, 'd, 'e> {
    kv_engine: &'a LsmKv,
    session: &'b mut SessionInner,
    col_name: &'c str,
    index_name: &'d str,
    index_info: &'e IndexInfo,
}

impl<'a, 'b, 'c, 'd, 'e> IndexBuilder<'a, 'b, 'c, 'd, 'e> {

    #[inline]
    pub fn new(
        kv_engine: &'a LsmKv,
        session: &'b mut SessionInner,
        col_name: &'c str,
        index_name: &'d str,
        index_info: &'e IndexInfo,
    ) -> IndexBuilder<'a, 'b, 'c, 'd, 'e> {
        IndexBuilder {
            kv_engine,
            session,
            col_name,
            index_name,
            index_info,
        }
    }

    pub fn execute(&mut self, op: IndexHelperOperation) -> Result<()> {
        let multi_cursor = self.kv_engine.open_multi_cursor(
            Some(self.session.kv_session()),
        );
        let mut cursor = Cursor::new_with_str_prefix(
            self.col_name.to_string(),
            multi_cursor,
        )?;

        cursor.reset()?;

        while cursor.has_next() {
            // get the value and insert index
            let current_data = cursor.peek_data(self.kv_engine.inner.as_ref()).unwrap().unwrap();

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
            &self.kv_engine,
            self.session,
        )
    }

}
