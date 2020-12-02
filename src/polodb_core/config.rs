
pub struct Config {
    pub init_block_count:  u64,
    pub journal_full_size: u64,
}

impl Default for Config {

    fn default() -> Self {
        Config {
            init_block_count:  16,
            journal_full_size: 1000,
        }
    }

}
