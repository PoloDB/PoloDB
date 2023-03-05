/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod backend;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod file;

pub(crate) mod memory;

#[cfg(target_arch = "wasm32")]
pub(crate) mod indexeddb;

pub(crate) use backend::{Backend, AutoStartResult};
