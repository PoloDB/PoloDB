/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
mod db;
mod collection;
mod db_inner;
pub mod client_cursor;
mod server;

pub use collection::Collection;
pub use db::{Database, DbResult};
pub use server::DatabaseServer;
