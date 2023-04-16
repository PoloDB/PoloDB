/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use crate::DbResult;
use crate::lsm::lsm_segment::LsmTuplePtr;
use crate::lsm::lsm_snapshot::LsmSnapshot;
use crate::lsm::mem_table::MemTable;

pub(crate) trait LsmBackend: Send + Sync {
    fn read_segment_by_ptr(&self, ptr: LsmTuplePtr) -> DbResult<Arc<[u8]>>;
    fn read_latest_snapshot(&self) -> DbResult<LsmSnapshot>;
    fn sync_latest_segment(&self, segment: &MemTable, snapshot: &mut LsmSnapshot) -> DbResult<()>;
    fn minor_compact(&self, snapshot: &mut LsmSnapshot) -> DbResult<()>;
    fn major_compact(&self, snapshot: &mut LsmSnapshot) -> DbResult<()>;
    fn checkpoint_snapshot(&self, snapshot: &mut LsmSnapshot) -> DbResult<()>;
}

pub(crate) mod lsm_backend_utils {
    use std::sync::Arc;
    use crate::DbResult;
    use crate::lsm::lsm_segment::LsmTuplePtr;
    use crate::lsm::lsm_tree::LsmTreeValueMarker;
    use crate::lsm::multi_cursor::MultiCursor;
    use crate::utils::vli;

    pub(crate) struct MergeLevelResult {
        pub tuples: Vec<(Arc<[u8]>, LsmTreeValueMarker<LsmTuplePtr>)>,
        pub estimate_size: usize,
    }

    pub(crate) fn merge_level(mut cursor: MultiCursor, preserve_delete: bool) -> DbResult<MergeLevelResult> {
        cursor.go_to_min()?;

        let mut tuples = Vec::<(Arc<[u8]>, LsmTreeValueMarker<LsmTuplePtr>)>::new();

        while !cursor.done() {
            let key_opt = cursor.key();
            match key_opt {
                Some(key) => {
                    let value = cursor.unwrap_tuple_ptr()?;

                    if preserve_delete {
                        tuples.push((key, value));
                        cursor.next()?;
                        continue;
                    }

                    if value.is_value() {
                        tuples.push((key, value));
                    }
                }
                None => break,
            }

            cursor.next()?;
        }

        let estimate_size = estimate_merge_tuples_byte_size(&tuples);

        Ok(MergeLevelResult {
            tuples,
            estimate_size,
        })
    }

    fn estimate_merge_tuples_byte_size(tuples: &[(Arc<[u8]>, LsmTreeValueMarker<LsmTuplePtr>)]) -> usize {
        let mut result: usize = 0;

        for (key, value) in tuples {
            let value_size = match value {
                LsmTreeValueMarker::Value(tuple) => tuple.byte_size as usize,
                _ => {
                    estimate_key_size(key)
                }
            };

            result += value_size;
        }

        result
    }

    pub(crate) fn estimate_key_size(key: &Arc<[u8]>) -> usize {

        let mut result: usize = 0;

        result += 1;

        result += vli::vli_len_u64(key.len() as u64);

        result += key.len();

        result
    }

}
