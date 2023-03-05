/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::btree::btree_v2::{BTreePageDelegate, BTreePageDelegateWithKey};
use crate::btree::BTreePageDeleteWrapper;
use crate::DbResult;
use crate::collection_info::CollectionSpecification;
use crate::session::Session;

pub(crate) fn delete_all(session: &dyn Session, col_spec: &CollectionSpecification) -> DbResult<()> {
    crate::polo_log!("delete all: {:?}", col_spec);
    delete_all_by_btree_pid(session, 0, col_spec.info.root_pid)
}

fn delete_all_by_btree_pid(session: &dyn Session, parent_id: u32, pid: u32) -> DbResult<()> {
    crate::polo_log!("delete all: parent pid: {}, pid: {}", parent_id, pid);
    let page = session.read_page(pid)?;
    let delegate = BTreePageDelegate::from_page(page.as_ref(), parent_id)?;
    if delegate.is_empty() {
        return Ok(())
    }
    let btree_node = BTreePageDelegateWithKey::read_from_session(delegate, session)?;
    let children_pid = btree_node.children_pid();

    for index in 0..btree_node.len() {
        BTreePageDeleteWrapper::indeed_delete_item_on_btree(session, btree_node.get_item(index))?;
    }

    for child_pid in children_pid {
        if child_pid == 0 {  // TODO: why?
            continue;
        }
        delete_all_by_btree_pid(session, pid, child_pid)?;
    }

    session.free_page(pid)?;

    Ok(())
}
