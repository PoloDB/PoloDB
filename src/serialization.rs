use std::io;
use crate::db::DbResult;

pub trait DbSerializer {

    fn serialize(&self, writer: &mut dyn io::Write) -> DbResult<()>;

}
