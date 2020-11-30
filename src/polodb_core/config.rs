
pub struct Config {
    pub journal_full_size: usize,
}

impl Default for Config {

    fn default() -> Self {
        Config {
            journal_full_size: 1000,
        }
    }

}
