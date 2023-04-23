/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fs::File;
use crate::{ErrorKind, Result};

#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawHandle;

#[cfg(target_os = "windows")]
pub(crate) fn exclusive_lock_file(file: &File) -> Result<()> {
    use winapi::um::fileapi::LockFileEx;
    use winapi::um::minwinbase::OVERLAPPED;
    use winapi::um::minwinbase::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY};
    use winapi::ctypes;

    let handle = file.as_raw_handle();

    let bl = unsafe {
        let overlapped: *mut OVERLAPPED = libc::malloc(std::mem::size_of::<OVERLAPPED>()).cast::<OVERLAPPED>();
        libc::memset(overlapped.cast::<libc::c_void>(), 0, std::mem::size_of::<OVERLAPPED>());
        let result: i32 = LockFileEx(
            handle.cast::<ctypes::c_void>(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0, 0, 0, overlapped);
        libc::free(overlapped.cast::<libc::c_void>());
        result
    };

    if bl == 0 {
        return Err(DbErr::Busy);
    }

    Ok(())
}

#[cfg(target_os = "windows")]
pub(crate) fn shared_lock_file(file: &File) -> Result<()> {
    use winapi::um::fileapi::LockFileEx;
    use winapi::um::minwinbase::OVERLAPPED;
    use winapi::um::minwinbase::LOCKFILE_FAIL_IMMEDIATELY;
    use winapi::ctypes;

    let handle = file.as_raw_handle();

    let bl = unsafe {
        let overlapped: *mut OVERLAPPED = libc::malloc(std::mem::size_of::<OVERLAPPED>()).cast::<OVERLAPPED>();
        libc::memset(overlapped.cast::<libc::c_void>(), 0, std::mem::size_of::<OVERLAPPED>());
        let result: i32 = LockFileEx(handle.cast::<ctypes::c_void>(), LOCKFILE_FAIL_IMMEDIATELY, 0, 0, 0, overlapped);
        libc::free(overlapped.cast::<libc::c_void>());
        result
    };

    if bl == 0 {
        return Err(DbErr::Busy);
    }

    Ok(())
}

#[cfg(target_os = "windows")]
pub(crate) fn unlock_file(file: &File) -> Result<()> {
    use winapi::um::fileapi::UnlockFileEx;
    use winapi::um::minwinbase::OVERLAPPED;
    use winapi::ctypes;

    let handle = file.as_raw_handle();

    let bl = unsafe {
        let overlapped: *mut OVERLAPPED = libc::malloc(std::mem::size_of::<OVERLAPPED>()).cast::<OVERLAPPED>();
        libc::memset(overlapped.cast::<libc::c_void>(), 0, std::mem::size_of::<OVERLAPPED>());
        let result: i32 = UnlockFileEx(handle.cast::<ctypes::c_void>(), 0, 0, 0, overlapped);
        libc::free(overlapped.cast::<libc::c_void>());
        result
    };

    if bl == 0 {
        return Err(DbErr::Busy);
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
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
        Err(ErrorKind::DatabaseOccupied.into())
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
        Err(ErrorKind::DatabaseOccupied.into())
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
        Err(ErrorKind::DatabaseOccupied.into())
    }
}
