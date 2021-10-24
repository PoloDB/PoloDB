use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use memmap::{MmapMut, MmapOptions};
use crate::DbResult;
use crate::journal::salt::{generate_a_nonzero_salt, generate_a_salt};

#[repr(C)]
pub(crate) struct InternalState {
    pub(crate) version:          [u8; 4],
    pub(crate) page_size:        u32,
    pub(crate) salt1:            u32,
    pub(crate) salt2:            u32,

    // origin_state
    pub(crate) db_file_size:     u64,

    // count of all frames
    pub(crate) count:            u32,

    pub(crate) map_size:         u32,
}

pub(crate) struct SharedState {
    file: File,
    path: PathBuf,
    mem:  MmapMut,
}

fn mk_shm_path(db_path: &Path) -> PathBuf {
    let mut buf = db_path.to_path_buf();
    let filename = buf.file_name().unwrap().to_str().unwrap();
    let new_filename = String::from(filename) + ".shm";
    buf.set_file_name(new_filename);
    buf
}

impl SharedState {

    pub(crate) fn open(db_path: &Path, db_file_size: u64, page_size: u32) -> DbResult<SharedState> {
        let path = mk_shm_path(db_path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let file_size = file.metadata()?.len();
        if file_size == 0 {
            SharedState::init_file(&file)?;
        }

        let mem = unsafe { MmapOptions::new().map_mut(&file)? };

        let mut state = SharedState {
            file,
            path,
            mem,
        };

        state.init_state(db_file_size, page_size)?;

        Ok(state)
    }

    fn init_file(file: &File) -> DbResult<()> {
        let state_size = std::mem::size_of::<InternalState>();
        file.set_len(state_size as u64)?;
        Ok(())
    }

    fn init_state(&mut self, db_file_size: u64, page_size: u32) -> DbResult<()> {
        let init_version: [u8; 4] = [0, 0, 1, 0];

        let mut_state = self.mut_state();
        mut_state.version.copy_from_slice(&init_version);
        mut_state.page_size = page_size;
        mut_state.salt1 = generate_a_salt();
        mut_state.salt2 = generate_a_nonzero_salt();
        mut_state.db_file_size = db_file_size;
        mut_state.count = 0;
        mut_state.map_size = 0;

        Ok(())
    }

    pub(crate) fn state(&self) -> &InternalState {
        unsafe {
            self.mem.as_ptr().cast::<InternalState>().as_ref().unwrap()
        }
    }

    pub(crate) fn mut_state(&mut self) -> &mut InternalState {
        unsafe {
            self.mem.as_mut_ptr().cast::<InternalState>().as_mut().unwrap()
        }
    }

}

impl Drop for SharedState {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use crate::journal::shared_state::InternalState;

    #[test]
    fn test_state_size() {
        assert_eq!(std::mem::size_of::<InternalState>(), 32);
    }

}
