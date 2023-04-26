/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bson::Bson;
use crate::Result;
use crate::collection_info::{CollectionSpecification, IndexInfo};
use crate::session::SessionInner;

const INDEX_PREFIX: &'static str = "$I";

pub(crate) struct IndexHelper<'a, 'b, 'c, 'd> {
    session: &'a mut SessionInner,
    col_spec: &'b CollectionSpecification,
    doc: &'c bson::Document,
    pkey: &'d Bson,
}

impl<'a, 'b, 'c, 'd> IndexHelper<'a, 'b, 'c, 'd> {

    #[inline]
    pub fn new(
        session: &'a mut SessionInner,
        col_spec: &'b CollectionSpecification,
        doc: &'c bson::Document,
        pkey: &'d Bson,
    ) -> IndexHelper<'a, 'b, 'c, 'd> {
        IndexHelper {
            session,
            col_spec,
            doc,
            pkey,
        }
    }

    pub fn execute(&mut self) -> Result<()> {
        let index_meta = &self.col_spec.indexes;

        let values = index_meta.iter().collect::<Vec<(&String, &IndexInfo)>>();

        for (index_name, index_info) in values {
            self.try_insert_index_with_index_info(
                index_name.as_str(),
                index_info,
            )?;
        }

        Ok(())
    }

    // The key of the collection value: collection_id + '\t' + primary_key
    // The key of the index in the table: '$I' + '\t' + collection_id + '\t' + index_name + '\t' + primary_key
    fn try_insert_index_with_index_info(
        &mut self,
        index_name: &str,
        index_info: &IndexInfo,
    ) -> Result<()> {
        let tuples = index_info.keys.iter().collect::<Vec<(&String, &i8)>>();
        let first_tuple = tuples.first().unwrap();
        let (keys, _order) = first_tuple;

        let value = crate::utils::bson::try_get_document_value(self.doc, keys);
        if value.is_none() {
            return Ok(())
        }

        let index_key = crate::utils::bson::stacked_key([
            &Bson::String(INDEX_PREFIX.to_string()),
            &Bson::String(self.col_spec._id.clone()),
            &Bson::String(index_name.to_string()),
            value.as_ref().unwrap(),
        ])?;

        let value = Bson::Array(vec![
            self.pkey.clone()
        ]);

        let value_buf = bson::to_vec(&value)?;

        self.session.put(index_key.as_slice(), value_buf.as_ref())?;

        Ok(())
    }

}
