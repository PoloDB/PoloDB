/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use bson::{Bson, Document};
use bson::spec::ElementType;
use crate::{LsmKv, Result};
use crate::coll::collection_info::{
    CollectionSpecification,
    IndexInfo,
};
use crate::errors::DuplicateKeyError;
use crate::session::SessionInner;

const INDEX_PREFIX: &'static str = "$I";

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum IndexHelperOperation {
    Insert,
    Delete,
}

pub(crate) struct IndexHelper<'a, 'b, 'c, 'd, 'e> {
    kv_engine: &'a LsmKv,
    session: &'b mut SessionInner,
    col_spec: &'c CollectionSpecification,
    doc: &'d Document,
    pkey: &'e Bson,
}

impl<'a, 'b, 'c, 'd, 'e> IndexHelper<'a, 'b, 'c, 'd, 'e> {

    #[inline]
    pub fn new(
        kv_engine: &'a LsmKv,
        session: &'b mut SessionInner,
        col_spec: &'c CollectionSpecification,
        doc: &'d Document,
        pkey: &'e Bson,
    ) -> IndexHelper<'a, 'b, 'c, 'd, 'e> {
        IndexHelper {
            kv_engine,
            session,
            col_spec,
            doc,
            pkey,
        }
    }

    pub fn execute(&mut self, op: IndexHelperOperation) -> Result<()> {
        let index_meta = &self.col_spec.indexes;

        let values = index_meta.iter().collect::<Vec<(&String, &IndexInfo)>>();

        for (index_name, index_info) in values {
            IndexHelper::try_execute_with_index_info(
                op,
                &self.doc,
                self.col_spec._id.as_str(),
                self.pkey,
                index_name.as_str(),
                index_info,
                self.kv_engine,
                self.session,
            )?;
        }

        Ok(())
    }

    // The key of the collection value: collection_id + '\t' + primary_key
    // The key of the index in the table: '$I' + '\t' + collection_id + '\t' + index_name + '\t' + primary_key
    pub(crate) fn try_execute_with_index_info(
        op: IndexHelperOperation,
        data_doc: &Document,
        col_name: &str,
        pkey: &Bson,
        index_name: &str,
        index_info: &IndexInfo,
        kv_engine: &'a LsmKv,
        session: &mut SessionInner,
    ) -> Result<()> {
        let tuples = index_info.keys.iter().collect::<Vec<(&String, &i8)>>();
        let first_tuple = tuples.first().unwrap();
        let (keys, _order) = first_tuple;

        let value = crate::utils::bson::try_get_document_value(data_doc, keys);
        if value.is_none() {
            return Ok(())
        }

        if index_info.is_unique() {
            IndexHelper::check_unique_key(
                col_name,
                index_name,
                value.as_ref().unwrap(),
                kv_engine,
                session,
            )?;
        }

        let index_key = IndexHelper::make_index_key(
            col_name,
            index_name,
            value.as_ref().unwrap(),
            Some(pkey),
        )?;

        if op == IndexHelperOperation::Delete {
            let value_buf = [ElementType::Null as u8];
            session.put(index_key.as_slice(), &value_buf)?;
        } else {
            session.delete(index_key.as_slice())?;
        }

        Ok(())
    }

    fn check_unique_key(
        col_name: &str,
        index_name: &str,
        value: &Bson,
        kv_engine: &'a LsmKv,
        session: &mut SessionInner,
    ) -> Result<()> {
        let index_key_tester = IndexHelper::make_index_key(
            col_name,
            index_name,
            value,
            None,
        )?;

        let mut cursor = kv_engine.open_multi_cursor(Some(session.kv_session()));
        cursor.seek(&index_key_tester)?;

        let current_key = cursor.key();
        if current_key.is_none() {
            return Ok(());
        }

        let current_key: Arc<[u8]> = current_key.unwrap();

        if current_key.starts_with(&index_key_tester) {
            return Err(DuplicateKeyError {
                name: index_name.to_string(),
                key: value.to_string(),
                ns: col_name.to_string(),
            }.into());
        }

        Ok(())
    }

    pub fn make_index_key(col_name: &str, index_name: &str, value: &Bson, pkey: Option<&Bson>) -> Result<Vec<u8>> {
        let b_prefix = Bson::String(INDEX_PREFIX.to_string());
        let b_col_name = Bson::String(col_name.to_string());
        let b_index_name = &Bson::String(index_name.to_string());

        let mut buf: Vec<&Bson> = vec![
            &b_prefix,
            &b_col_name,
            &b_index_name,
            value,
        ];

        if let Some(pkey) = pkey {
            buf.push(pkey);
        }

        crate::utils::bson::stacked_key(buf)
    }

}

#[cfg(test)]
mod tests {
    use bson::Bson;
    use super::IndexHelper;

    #[test]
    fn test_make_index_key() {
        let index_key = IndexHelper::make_index_key(
            "users",
            "name",
            &Bson::String("value".to_string()),
            Some(&Bson::String("Vincent".to_string())),
        ).unwrap() ;

        let escaped_string = String::from_utf8(
        index_key
            .iter()
            .flat_map(|b| std::ascii::escape_default(*b))
            .collect::<Vec<u8>>(),
        )
        .unwrap();

        assert_eq!(escaped_string, "\\x02$I\\x00\\x02users\\x00\\x02name\\x00\\x02value\\x00\\x02Vincent\\x00");
    }

}
