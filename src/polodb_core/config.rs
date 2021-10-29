
use crate::doc_serializer::SerializeType;
use std::num::NonZeroU64;

pub struct Config {
    pub init_block_count:  NonZeroU64,
    pub journal_full_size: u64,
    pub serialize_type:    SerializeType,
}

impl Default for Config {

    fn default() -> Self {
        Config {
            init_block_count:  NonZeroU64::new(16).unwrap(),
            journal_full_size: 1000,
            serialize_type:    SerializeType::Default,
        }
    }

}
