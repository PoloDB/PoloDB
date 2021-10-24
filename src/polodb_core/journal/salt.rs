use libc::rand;

pub(super) fn generate_a_salt() -> u32 {
    unsafe {
        rand() as u32
    }
}

pub(super) fn generate_a_nonzero_salt() -> u32 {
    let mut salt = generate_a_salt();
    while salt == 0 {
        salt = generate_a_salt();
    }
    salt
}
