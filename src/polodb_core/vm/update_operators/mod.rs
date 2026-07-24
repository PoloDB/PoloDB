mod document_path;
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
        let keys: Vec<_> = doc.keys().collect();
        for (index, k) in keys.iter().enumerate() {
            document_path::validate_update_path(k)?;
            for other in keys.iter().skip(index + 1) {
                if document_path::paths_conflict(k, other) {
                    return Err(crate::Error::ValidationError(format!(
                        "conflicting update paths '{k}' and '{other}'"
                    )));
                }
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
