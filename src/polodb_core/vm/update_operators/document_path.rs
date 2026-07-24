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

use bson::{Bson, Document};

use crate::errors::FieldTypeUnexpectedStruct;
use crate::Result;

fn split_path(path: &str) -> Result<Vec<&str>> {
    let parts: Vec<_> = path.split('.').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return Err(crate::Error::ValidationError(format!(
            "update path '{path}' contains an empty field name"
        )));
    }
    Ok(parts)
}

fn non_document_path_error(path: &str, value: &Bson) -> crate::Error {
    FieldTypeUnexpectedStruct {
        field_name: path.into(),
        expected_ty: "Document".into(),
        actual_ty: value.to_string(),
    }
    .into()
}

pub(super) fn validate_update_path(path: &str) -> Result<()> {
    let parts = split_path(path)?;
    if parts.first() == Some(&"_id") {
        return Err(crate::Error::UnableToUpdatePrimaryKey);
    }
    Ok(())
}

pub(super) fn paths_conflict(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('.'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

pub(super) fn get_path<'a>(doc: &'a Document, path: &str) -> Result<Option<&'a Bson>> {
    let parts = split_path(path)?;
    let mut current = doc;

    for (index, part) in parts.iter().enumerate() {
        let value = match current.get(*part) {
            Some(value) => value,
            None => return Ok(None),
        };
        if index == parts.len() - 1 {
            return Ok(Some(value));
        }
        current = value
            .as_document()
            .ok_or_else(|| non_document_path_error(path, value))?;
    }

    unreachable!("split_path always returns at least one component")
}

fn parent_mut<'a>(
    doc: &'a mut Document,
    path: &str,
    create: bool,
) -> Result<(&'a mut Document, String)> {
    let parts = split_path(path)?;
    let (field, parents) = parts.split_last().unwrap();
    let mut current = doc;

    for part in parents {
        if !current.contains_key(*part) {
            if !create {
                return Ok((current, field.to_string()));
            }
            current.insert(*part, Bson::Document(Document::new()));
        }

        let value = current.get_mut(*part).unwrap();
        current = match value {
            Bson::Document(document) => document,
            other => return Err(non_document_path_error(path, other)),
        };
    }

    Ok((current, field.to_string()))
}

pub(super) fn set_path(doc: &mut Document, path: &str, value: Bson) -> Result<Option<Bson>> {
    let (parent, field) = parent_mut(doc, path, true)?;
    Ok(parent.insert(field, value))
}

pub(super) fn remove_path(doc: &mut Document, path: &str) -> Result<Option<Bson>> {
    if get_path(doc, path)?.is_none() {
        return Ok(None);
    }
    let (parent, field) = parent_mut(doc, path, false)?;
    Ok(parent.remove(&field))
}

pub(super) fn rename_path(doc: &mut Document, source: &str, destination: &str) -> Result<bool> {
    let mut updated = doc.clone();
    let Some(value) = remove_path(&mut updated, source)? else {
        return Ok(false);
    };
    set_path(&mut updated, destination, value)?;
    *doc = updated;
    Ok(true)
}
