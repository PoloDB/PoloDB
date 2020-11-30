
// 24 bytes
pub(super) struct FrameHeader {
    // the page_id of the main database
    // page_id * offset represents the real offset from the beginning
    pub(super) page_id:       u32,  // offset 0

    // usually 0
    // if this frame is the final commit of a transaction
    // this field represents the read db_size
    pub(super) db_size:       u64,  // offset 8

    // should be the same as the header of journal file
    // is they are not equal, abandon this frame
    pub(super) salt1:         u32,  // offset 16
    pub(super) salt2:         u32,  // offset 20
}

impl FrameHeader {

    pub(super) fn from_bytes(bytes: &[u8]) -> FrameHeader {
        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&bytes[0..4]);

        let page_id = u32::from_be_bytes(buffer);

        let mut buffer: [u8; 8] = [0; 8];
        buffer.copy_from_slice(&bytes[8..16]);
        let db_size = u64::from_be_bytes(buffer);

        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&bytes[16..20]);
        let salt1 = u32::from_be_bytes(buffer);

        let mut buffer: [u8; 4] = [0; 4];
        buffer.copy_from_slice(&bytes[20..24]);
        let salt2 = u32::from_be_bytes(buffer);

        FrameHeader {
            page_id,
            db_size,
            salt1, salt2
        }
    }

    pub(super) fn to_bytes(&self, buffer: &mut [u8]) {
        debug_assert!(buffer.len() >= 24);
        let page_id_be = self.page_id.to_be_bytes();
        buffer[0..4].copy_from_slice(&page_id_be);

        let db_size_be = self.db_size.to_be_bytes();
        buffer[8..16].copy_from_slice(&db_size_be);

        let salt1_be = self.salt1.to_be_bytes();
        buffer[16..20].copy_from_slice(&salt1_be);

        let salt2_be = self.salt2.to_be_bytes();
        buffer[20..24].copy_from_slice(&salt2_be);
    }

}
