
// 6 bytes in store
#[derive(Clone)]
pub(crate) struct DataTicket {
    pub pid: u32,
    pub index: u16,
}

impl DataTicket {

    pub fn to_bytes(&self) -> [u8; 6] {
        let mut result = [0; 6];

        let pid_bytes = self.pid.to_be_bytes();
        let index_bytes = self.index.to_be_bytes();

        result[0..4].copy_from_slice(&pid_bytes);
        result[4..6].copy_from_slice(&index_bytes);

        result
    }

    pub fn from_bytes(bytes: &[u8]) -> DataTicket {
        let mut pid_bytes = [0; 4];
        let mut index_bytes = [0; 2];

        pid_bytes.copy_from_slice(&bytes[0..4]);
        index_bytes.copy_from_slice(&bytes[4..6]);

        let pid = u32::from_be_bytes(pid_bytes);
        let index = u16::from_be_bytes(index_bytes);

        DataTicket { pid, index }
    }

}
