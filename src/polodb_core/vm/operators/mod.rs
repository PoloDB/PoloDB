mod sum_operator;

use bson::Bson;

pub(crate) trait VmOperator {

    fn initial_value(&self) -> Bson;

    fn next(&self, input: &Bson) -> Bson;

    fn complete(&self) -> Bson;

}

pub(crate) use sum_operator::SumOperator;
