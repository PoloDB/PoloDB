// Copyright 2024 Vincent Chan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bson::{Bson, Document, Regex};
use regex::{Regex as CompiledRegex, RegexBuilder};

use crate::errors::RegexError;
use crate::{Error, Result};

pub(super) fn field_path_value(document: &Document, path: &str) -> Option<Bson> {
    let mut segments = path.split('.');
    let first = segments.next()?;
    let mut value = document.get(first)?;

    for segment in segments {
        value = value.as_document()?.get(segment)?;
    }

    Some(value.clone())
}

pub(super) fn matches_in(field_value: &Bson, candidates: &[Bson]) -> Result<bool> {
    let compiled_regexes = compile_in_regexes(candidates)?;

    for (candidate, compiled_regex) in candidates.iter().zip(compiled_regexes.iter()) {
        if query_values_equal(field_value, candidate) {
            return Ok(true);
        }

        if let Bson::Array(values) = field_value {
            if values
                .iter()
                .any(|value| query_values_equal(value, candidate))
            {
                return Ok(true);
            }
        }

        if compiled_regex
            .as_ref()
            .is_some_and(|regex| matches_compiled_regex(field_value, regex))
        {
            return Ok(true);
        }
    }

    Ok(false)
}

pub(super) fn validate_in_candidates(candidates: &[Bson]) -> Result<()> {
    compile_in_regexes(candidates).map(|_| ())
}

pub(super) fn matches_regex(field_value: &Bson, expression: &Regex) -> Result<bool> {
    let regex = compile_regex(expression)?;
    Ok(regex.is_match(&field_value.to_string()))
}

fn matches_compiled_regex(field_value: &Bson, regex: &CompiledRegex) -> bool {
    match field_value {
        Bson::String(value) | Bson::Symbol(value) => regex.is_match(value),
        Bson::Array(values) => values.iter().any(|value| match value {
            Bson::String(value) | Bson::Symbol(value) => regex.is_match(value),
            _ => false,
        }),
        _ => false,
    }
}

fn compile_in_regexes(candidates: &[Bson]) -> Result<Vec<Option<CompiledRegex>>> {
    candidates
        .iter()
        .map(|candidate| match candidate {
            Bson::RegularExpression(expression) => compile_regex(expression).map(Some),
            _ => Ok(None),
        })
        .collect()
}

fn compile_regex(expression: &Regex) -> Result<CompiledRegex> {
    let mut builder = RegexBuilder::new(expression.pattern.as_str());

    for option in expression.options.chars() {
        match option {
            'i' => builder.case_insensitive(true),
            'm' => builder.multi_line(true),
            's' => builder.dot_matches_new_line(true),
            'u' => builder.unicode(true),
            'U' => builder.swap_greed(true),
            'x' => builder.ignore_whitespace(true),
            _ => {
                return Err(Error::from(RegexError {
                    error: format!("unknown regex option: {option}"),
                    expression: expression.pattern.clone(),
                    options: expression.options.clone(),
                }));
            }
        };
    }

    builder.build().map_err(|error| {
        Error::from(RegexError {
            error: format!("regex build error: {error}"),
            expression: expression.pattern.clone(),
            options: expression.options.clone(),
        })
    })
}

fn query_values_equal(left: &Bson, right: &Bson) -> bool {
    match (left, right) {
        (
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_),
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_),
        ) => numeric_values_equal(left, right),
        (Bson::Array(left), Bson::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right.iter())
                    .all(|(left, right)| query_values_equal(left, right))
        }
        (Bson::Document(left), Bson::Document(right)) => documents_equal(left, right),
        (Bson::JavaScriptCodeWithScope(left), Bson::JavaScriptCodeWithScope(right)) => {
            left.code == right.code && documents_equal(&left.scope, &right.scope)
        }
        (Bson::Binary(left), Bson::Binary(right)) => {
            left.subtype == right.subtype && left.bytes == right.bytes
        }
        (Bson::String(left), Bson::Symbol(right)) | (Bson::Symbol(left), Bson::String(right)) => {
            left == right
        }
        _ => left == right,
    }
}

fn numeric_values_equal(left: &Bson, right: &Bson) -> bool {
    match (left, right) {
        (Bson::Double(left), Bson::Double(right)) => {
            left == right || (left.is_nan() && right.is_nan())
        }
        (Bson::Double(double), Bson::Int32(integer))
        | (Bson::Int32(integer), Bson::Double(double)) => {
            integer_equals_double(i64::from(*integer), *double)
        }
        (Bson::Double(double), Bson::Int64(integer))
        | (Bson::Int64(integer), Bson::Double(double)) => integer_equals_double(*integer, *double),
        (Bson::Int32(left), Bson::Int32(right)) => left == right,
        (Bson::Int32(left), Bson::Int64(right)) => i64::from(*left) == *right,
        (Bson::Int64(left), Bson::Int32(right)) => *left == i64::from(*right),
        (Bson::Int64(left), Bson::Int64(right)) => left == right,
        _ => false,
    }
}

fn integer_equals_double(integer: i64, double: f64) -> bool {
    const I64_LOWER_BOUND: f64 = -9_223_372_036_854_775_808.0;
    const I64_UPPER_BOUND: f64 = 9_223_372_036_854_775_808.0;

    double.is_finite()
        && double.trunc() == double
        && (I64_LOWER_BOUND..I64_UPPER_BOUND).contains(&double)
        && double as i64 == integer
}

fn documents_equal(left: &Document, right: &Document) -> bool {
    left.len() == right.len()
        && left.iter().zip(right.iter()).all(
            |((left_key, left_value), (right_key, right_value))| {
                left_key == right_key && query_values_equal(left_value, right_value)
            },
        )
}

#[cfg(test)]
mod tests {
    use bson::spec::BinarySubtype;
    use bson::{doc, Binary, Bson, JavaScriptCodeWithScope, Regex};

    use super::{field_path_value, matches_in, query_values_equal};

    #[test]
    fn field_path_lookup_preserves_terminal_documents() {
        let document = doc! {
            "profile": {
                "name": "Ada",
            },
        };

        assert_eq!(
            field_path_value(&document, "profile"),
            document.get("profile").cloned(),
        );
        assert_eq!(
            field_path_value(&document, "profile.name"),
            Some(Bson::String("Ada".into())),
        );
    }

    #[test]
    fn query_equality_is_recursive_and_ordered() {
        assert!(query_values_equal(
            &Bson::Array(vec![Bson::Int32(1)]),
            &Bson::Array(vec![Bson::Int64(1)]),
        ));

        assert!(!query_values_equal(
            &Bson::Document(doc! { "a": 1, "b": 2 }),
            &Bson::Document(doc! { "b": 2, "a": 1 }),
        ));
    }

    #[test]
    fn numeric_equality_is_exact_across_bson_types() {
        assert!(query_values_equal(&Bson::Double(-0.0), &Bson::Int32(0)));
        assert!(query_values_equal(&Bson::Double(0.0), &Bson::Double(-0.0)));
        assert!(query_values_equal(
            &Bson::Double(f64::NAN),
            &Bson::Double(f64::NAN),
        ));
        assert!(!query_values_equal(
            &Bson::Int64(9_007_199_254_740_993),
            &Bson::Double(9_007_199_254_740_992.0),
        ));
        assert!(!query_values_equal(
            &Bson::Int64(i64::MAX),
            &Bson::Double(9_223_372_036_854_775_808.0),
        ));
        assert!(query_values_equal(
            &Bson::Int64(i64::MIN),
            &Bson::Double(-9_223_372_036_854_775_808.0),
        ));
    }

    #[test]
    fn binary_equality_includes_subtype() {
        let bytes = vec![1, 2, 3];
        assert!(!query_values_equal(
            &Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: bytes.clone(),
            }),
            &Bson::Binary(Binary {
                subtype: BinarySubtype::Uuid,
                bytes,
            }),
        ));
    }

    #[test]
    fn code_with_scope_uses_recursive_ordered_document_equality() {
        let stored = Bson::JavaScriptCodeWithScope(JavaScriptCodeWithScope {
            code: "return a".into(),
            scope: doc! { "a": Bson::Int32(1), "b": "x" },
        });
        let equivalent = Bson::JavaScriptCodeWithScope(JavaScriptCodeWithScope {
            code: "return a".into(),
            scope: doc! { "a": Bson::Int64(1), "b": Bson::Symbol("x".into()) },
        });
        let reversed = Bson::JavaScriptCodeWithScope(JavaScriptCodeWithScope {
            code: "return a".into(),
            scope: doc! { "b": Bson::Symbol("x".into()), "a": Bson::Int64(1) },
        });

        assert!(query_values_equal(&stored, &equivalent));
        assert!(!query_values_equal(&stored, &reversed));
    }

    #[test]
    fn regex_candidates_only_match_strings() {
        let candidate = Bson::RegularExpression(Regex {
            pattern: "^foo".into(),
            options: "i".into(),
        });

        assert!(matches_in(&Bson::String("Food".into()), &[candidate.clone()]).unwrap());
        assert!(matches_in(
            &Bson::Array(vec![Bson::String("Food".into())]),
            &[candidate.clone()],
        )
        .unwrap());
        assert!(!matches_in(&Bson::Int32(42), &[candidate]).unwrap());
    }

    #[test]
    fn all_regex_candidates_are_validated_before_matching() {
        let candidates = [
            Bson::Int32(1),
            Bson::RegularExpression(Regex {
                pattern: "[".into(),
                options: "".into(),
            }),
        ];

        assert!(matches_in(&Bson::Int32(1), &candidates).is_err());
    }

    #[test]
    fn array_candidates_match_whole_or_nested_arrays() {
        let candidate = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);

        assert!(matches_in(
            &Bson::Array(vec![Bson::Int64(1), Bson::Double(2.0)]),
            &[candidate.clone()],
        )
        .unwrap());
        assert!(matches_in(
            &Bson::Array(vec![candidate.clone(), Bson::String("x".into())]),
            &[candidate],
        )
        .unwrap());
    }
}
