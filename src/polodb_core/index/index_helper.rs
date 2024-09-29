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

use bson::{Bson, Document};
use bson::spec::ElementType;
use crate::Result;
use crate::coll::collection_info::{
    CollectionSpecification,
    IndexInfo,
};
use crate::errors::DuplicateKeyError;
use crate::transaction::TransactionInner;

pub(crate) const INDEX_PREFIX: &'static str = "$I";

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum IndexHelperOperation {
    Insert,
    Delete,
}

pub(crate) struct IndexHelper<'b, 'c, 'd, 'e> {
    txn: &'b TransactionInner,
    col_spec: &'c CollectionSpecification,
    doc: &'d Document,
    pkey: &'e Bson,
}

pub(crate) fn make_index_key_with_query_key(prefix_bytes: &[u8], query_value: &Bson) -> Result<Vec<u8>> {
    let mut key_buffer = prefix_bytes.to_vec();
    let primary_key_buffer = crate::utils::bson::stacked_key([
        query_value,
    ])?;

    key_buffer.extend_from_slice(&primary_key_buffer);

    Ok(key_buffer)
}

impl<'b, 'c, 'd, 'e> IndexHelper<'b, 'c, 'd, 'e> {

    #[inline]
    pub fn new(
        txn: &'b TransactionInner,
        col_spec: &'c CollectionSpecification,
        doc: &'d Document,
        pkey: &'e Bson,
    ) -> IndexHelper<'b, 'c, 'd, 'e> {
        IndexHelper {
            txn,
            col_spec,
            doc,
            pkey,
        }
    }

    pub fn execute(&mut self, op: IndexHelperOperation) -> Result<()> {
        let index_meta = &self.col_spec.indexes;

        for (index_name, index_info) in index_meta.iter() {
            IndexHelper::try_execute_with_index_info(
                op,
                &self.doc,
                self.col_spec._id.as_str(),
                self.pkey,
                index_name.as_str(),
                index_info,
                self.txn,
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
        txn: &TransactionInner,
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
                txn,
            )?;
        }

        let index_key = IndexHelper::make_index_key(
            col_name,
            index_name,
            value.as_ref().unwrap(),
            Some(pkey),
        )?;

        if op == IndexHelperOperation::Insert {
            let value_buf = [ElementType::Null as u8];
            txn.put(index_key.as_slice(), &value_buf)?;
        } else {
            txn.delete(index_key.as_slice())?;
        }

        Ok(())
    }

    fn check_unique_key(
        col_name: &str,
        index_name: &str,
        value: &Bson,
        txn: &TransactionInner,
    ) -> Result<()> {
        let index_key_tester = IndexHelper::make_index_key(
            col_name,
            index_name,
            value,
            None,
        )?;

        let cursor = txn.rocksdb_txn.new_iterator();
        cursor.seek(&index_key_tester);

        if !cursor.valid() {
            return Ok(());
        }
        let current_key = cursor.copy_key_arc()?;

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
    use crate::utils::str::escape_binary_to_string;
    use super::IndexHelper;

    #[test]
    fn test_make_index_key() {
        let index_key = IndexHelper::make_index_key(
            "users",
            "name",
            &Bson::String("value".to_string()),
            Some(&Bson::String("Vincent".to_string())),
        ).unwrap() ;

        let escaped_string = escape_binary_to_string(index_key) .unwrap();

        assert_eq!(escaped_string, "\\x02$I\\x00\\x02users\\x00\\x02name\\x00\\x02value\\x00\\x02Vincent\\x00");
    }

}
