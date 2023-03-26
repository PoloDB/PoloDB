/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

pub(crate) enum LsmTreeValueMarker<V> {
    Deleted,
    DeleteStart,
    DeleteEnd,
    Value(V),
}

impl<V> LsmTreeValueMarker<V> {

    #[inline]
    #[allow(dead_code)]
    pub fn is_delete_start(&self) -> bool {
        match self {
            LsmTreeValueMarker::DeleteStart => true,
            _ => false,
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_delete_end(&self) -> bool {
        match self {
            LsmTreeValueMarker::DeleteEnd => true,
            _ => false,
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_deleted(&self) -> bool {
        match self {
            LsmTreeValueMarker::Deleted => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_value(&self) -> bool {
        match self {
            LsmTreeValueMarker::Value(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn unwrap(self) -> V {
        match self {
            LsmTreeValueMarker::Value(value) => value,
            _ => {
                panic!("this marker is no value: {}", self)
            }
        }
    }

    pub fn as_ref<T: ?Sized>(&self) -> LsmTreeValueMarker<&T>
    where
        V: AsRef<T>
    {
        match self {
            LsmTreeValueMarker::Deleted => LsmTreeValueMarker::Deleted,
            LsmTreeValueMarker::DeleteStart => LsmTreeValueMarker::DeleteStart,
            LsmTreeValueMarker::DeleteEnd => LsmTreeValueMarker::DeleteEnd,
            LsmTreeValueMarker::Value(v) => {
                LsmTreeValueMarker::Value(v.as_ref())
            }
        }
    }

}

impl<V: Clone> Clone for LsmTreeValueMarker<V> {
    fn clone(&self) -> Self {
        match self {
            LsmTreeValueMarker::Deleted => LsmTreeValueMarker::Deleted,
            LsmTreeValueMarker::DeleteStart => LsmTreeValueMarker::DeleteStart,
            LsmTreeValueMarker::DeleteEnd => LsmTreeValueMarker::DeleteEnd,
            LsmTreeValueMarker::Value(value) => {
                LsmTreeValueMarker::Value(value.clone())
            }
        }
    }
}

impl<V> Into<Option<V>> for LsmTreeValueMarker<V> {

    fn into(self) -> Option<V> {
        match self {
            LsmTreeValueMarker::Value(value) => Some(value),
            _ => None,
        }
    }

}

impl<T> Display for LsmTreeValueMarker<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LsmTreeValueMarker::Deleted => write!(f, "Deleted"),
            LsmTreeValueMarker::DeleteStart => write!(f, "DeleteStart"),
            LsmTreeValueMarker::DeleteEnd => write!(f, "DeleteEnd"),
            LsmTreeValueMarker::Value(_) => write!(f, "Value(_)"),
        }
    }
}
