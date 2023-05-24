/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::Result;

pub fn escape_binary_to_string<B: AsRef<[u8]>>(bytes: B) -> Result<String> {
    let escaped_string = String::from_utf8(
        bytes
            .as_ref()
            .iter()
            .flat_map(|b| std::ascii::escape_default(*b))
            .collect::<Vec<u8>>(),
    )?;
    Ok(escaped_string)
}
