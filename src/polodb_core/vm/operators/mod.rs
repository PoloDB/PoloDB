mod sum_operator;
mod op_registry;

use bson::Bson;

pub(crate) trait VmOperator {

    fn initial_value(&self) -> Bson;

    fn next(&self, input: &Bson) -> Bson;

    fn complete(&self) -> Bson;

}

pub(crate) use sum_operator::SumOperator;
pub(crate) use op_registry::OpRegistry;
