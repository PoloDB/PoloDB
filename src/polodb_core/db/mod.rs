/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod db;
mod collection;
mod context;
pub mod db_handle;

pub use collection::Collection;
pub use db::{Database, DbResult, IndexedDbContext};
pub(crate) use context::SessionInner;
pub(crate) use db::SHOULD_LOG;
