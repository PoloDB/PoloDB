mod set_operator;
mod inc_operator;
mod mul_operator;
mod rename_operator;
mod unset_operator;
mod push_operator;
mod pop_operator;
mod min_operator;
mod max_operator;

use bson::{Bson, Document};
use crate::Result;

#[derive(Debug, Default)]
pub(crate) struct UpdateResult {
    #[allow(dead_code)]
    pub(crate) updated: bool,
}

pub(crate) trait UpdateOperator {
    fn name(&self) -> &str;
    fn update(&self, value: &mut Bson) -> Result<UpdateResult>;
}

impl dyn UpdateOperator {

    pub(crate) fn validate_key(doc: &Document) -> Result<()> {
        for (k, _) in doc.iter() {
            if k == "_id" {
                return Err(crate::Error::UnableToUpdatePrimaryKey);
            }
        }
        Ok(())
    }

}

pub(crate) use set_operator::SetOperator;
pub(crate) use inc_operator::IncOperator;
pub(crate) use mul_operator::MulOperator;
pub(crate) use rename_operator::RenameOperator;
pub(crate) use unset_operator::UnsetOperator;
pub(crate) use push_operator::PushOperator;
pub(crate) use pop_operator::PopOperator;
pub(crate) use min_operator::MinOperator;
pub(crate) use max_operator::MaxOperator;
