/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

#[derive(Clone)]
pub(crate) enum LsmTreeValueMarker<V: Clone> {
    Deleted,
    DeleteStart,
    DeleteEnd,
    Value(V),
}

impl<V: Clone> LsmTreeValueMarker<V> {

    pub fn as_ref(&self) -> LsmTreeValueMarker<&V> {
        match self {
            LsmTreeValueMarker::Deleted => LsmTreeValueMarker::Deleted,
            LsmTreeValueMarker::DeleteStart => LsmTreeValueMarker::DeleteStart,
            LsmTreeValueMarker::DeleteEnd => LsmTreeValueMarker::DeleteEnd,
            LsmTreeValueMarker::Value(v) => LsmTreeValueMarker::Value(&v),
        }
    }

    pub fn unwrap(self) -> V {
        match self {
            LsmTreeValueMarker::Value(value) => value,
            _ => {
                panic!("this marker is no value: {}", self)
            }
        }
    }

}

impl<V: Clone> Into<Option<V>> for LsmTreeValueMarker<V> {

    fn into(self) -> Option<V> {
        match self {
            LsmTreeValueMarker::Value(value) => Some(value),
            _ => None,
        }
    }

}

impl<T: Clone> Display for LsmTreeValueMarker<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LsmTreeValueMarker::Deleted => write!(f, "Deleted"),
            LsmTreeValueMarker::DeleteStart => write!(f, "DeleteStart"),
            LsmTreeValueMarker::DeleteEnd => write!(f, "DeleteEnd"),
            LsmTreeValueMarker::Value(_) => write!(f, "Value(_)"),
        }
    }
}
