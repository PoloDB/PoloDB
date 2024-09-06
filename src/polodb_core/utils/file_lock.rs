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

#[cfg(not(target_os = "windows"))]
use std::fs::File;

#[cfg(not(target_os = "windows"))]
use crate::{Error, Result};

#[cfg(not(target_os = "windows"))]
#[allow(dead_code)]
pub(crate) fn exclusive_lock_file(file: &File) -> Result<()> {
    use std::os::unix::prelude::*;
    use libc::{flock, LOCK_EX, LOCK_NB};

    let fd = file.as_raw_fd();
    let result = unsafe {
        flock(fd, LOCK_EX | LOCK_NB)
    };

    if result == 0 {
        Ok(())
    } else {
        Err(Error::Busy)
    }
}

#[cfg(not(target_os = "windows"))]
#[allow(dead_code)]
pub(crate) fn shared_lock_file(file: &File) -> Result<()> {
    use std::os::unix::prelude::*;
    use libc::{flock, LOCK_SH, LOCK_NB};

    let fd = file.as_raw_fd();
    let result = unsafe {
        flock(fd, LOCK_SH | LOCK_NB)
    };

    if result == 0 {
        Ok(())
    } else {
        Err(Error::Busy)
    }
}

/// LOCK_UN: unlock
/// LOCK_NB: non-blocking
#[cfg(not(target_os = "windows"))]
#[allow(dead_code)]
pub(crate) fn unlock_file(file: &File) -> Result<()> {
    use std::os::unix::prelude::*;
    use libc::{flock, LOCK_UN, LOCK_NB};

    let fd = file.as_raw_fd();
    let result = unsafe {
        flock(fd, LOCK_UN | LOCK_NB)
    };

    if result == 0 {
        Ok(())
    } else {
        Err(Error::Busy)
    }
}
