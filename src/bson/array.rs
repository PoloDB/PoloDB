use std::io::Write;
use super::value;
use crate::serialization::DbSerializer;
use crate::db::DbResult;
use crate::error::DbErr;

#[derive(Debug, Clone)]
pub struct Array {
    pub data: Vec<value::Value>,
}

impl Array {

    pub fn new() -> Array {
        let data = vec![];
        Array { data }
    }

    pub fn len(&self) -> u32 {
        self.data.len() as u32
    }

}

impl DbSerializer for Array {

    fn serialize(&self, writer: &mut dyn Write) -> DbResult<()> {
        Err(DbErr::NotImplement)
    }

}
