use crate::{db::Result, Config};
use std::path::Path;

pub trait Backend
where
    Self: Sized,
{
    fn try_open(path: &Path) -> Result<Self>;
    fn try_open_with_config(path: &Path, config: Config) -> Result<Self>;
}
