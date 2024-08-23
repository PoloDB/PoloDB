mod sum_operator;
mod op_registry;
mod abs_operator;

use bson::Bson;

pub(crate) trait VmOperator {

    fn initial_value(&self) -> Bson;

    fn next(&self, input: &Bson) -> Bson;

    fn complete(&self) -> Bson;

}

pub(crate) enum OperatorExpr {
    Constant(Bson),
    Expr(Box<dyn VmOperator>),
    Alias(String),
}

pub(crate) use sum_operator::SumOperator;
pub(crate) use abs_operator::AbsOperator;
pub(crate) use op_registry::OpRegistry;
