/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use lz4_flex::{
    compress_prepend_size,
    decompress_size_prepended,
};
use lz4_flex::block::DecompressError;
use crate::lsm::lsm_segment::ImLsmSegment;
use crate::lsm::lsm_snapshot::{FreeSegmentRecord, LsmLevel, LsmSnapshot};
use crate::lsm::lsm_tree::LsmTree;

#[derive(Serialize, Deserialize)]
pub(crate) struct IdbSegment {
    #[serde(serialize_with = "bson::serde_helpers::serialize_object_id_as_hex_string")]
    pub id: ObjectId,
    pub compress: Option<String>,
    pub data: Vec<u8>,
}

impl IdbSegment {

    pub fn compress(id: ObjectId, data: &[u8]) -> IdbSegment {
        let compressed = compress_prepend_size(data);
        IdbSegment {
            id,
            compress: Some("lz4".into()),
            data: compressed,
        }
    }

    pub fn decompress(&self) -> Result<Vec<u8>, DecompressError> {
        match self.compress {
            Some(_) => {
                decompress_size_prepended(&self.data)
            }
            None => Ok(self.data.clone()),
        }
    }

}

#[derive(Serialize, Deserialize)]
pub(crate) struct IdbLog {
    pub content: Vec<u8>,
    #[serde(serialize_with = "bson::serde_helpers::serialize_object_id_as_hex_string")]
    pub session: ObjectId,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct IdbLevel {
    pub age: u16,
    // the key of the segments
    pub segments: Vec<ObjectId>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct IdbMeta {
    pub id:            u64,
    pub levels:        Vec<IdbLevel>,
    pub free_segments: Vec<FreeSegmentRecord>,
    #[serde(serialize_with = "bson::serde_helpers::serialize_object_id_as_hex_string")]
    pub session_id:    ObjectId,
}

impl IdbMeta {

    pub fn from_snapshot(session_id: ObjectId, snapshot: &LsmSnapshot) -> Self {
        let levels: Vec<IdbLevel> = snapshot
            .levels
            .iter()
            .map(|level| {
                let segments = level
                    .content
                    .iter()
                    .map(|segment| {
                        segment.to_object_id()
                    })
                    .collect();
                IdbLevel {
                    age: level.age,
                    segments,
                }
            })
            .collect();

        IdbMeta {
            id: snapshot.meta_id,
            levels,
            free_segments: snapshot.free_segments.clone(),
            session_id,
        }
    }

    pub fn generate_snapshot(&self) -> LsmSnapshot {
        let levels = self
            .levels
            .iter()
            .map(|level| {
                let content = level
                    .segments
                    .iter()
                    .map(|segment| {
                        // TODO: read segments
                        ImLsmSegment::from_object_id(
                            LsmTree::new(),
                            segment,
                        )
                    })
                    .collect();

                LsmLevel {
                    age: level.age,
                    content,
                }
            })
            .collect();

        LsmSnapshot {
            meta_pid: 0,
            meta_id: self.id,
            file_size: 0,
            log_offset: 0,
            free_segments: self.free_segments.clone(),
            levels,
        }
    }

}
