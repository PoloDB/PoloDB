use std::fs::File;
use std::io;
use crate::page::RawPage;

enum JournalType  {
    Invalid = 0,

    NewPage,

    WritePage,

    DeletePage,

}

struct Journal {
    ty: JournalType,
    __reserved0: u16,
    current_jid: i32,
    origin_jid: i64,
}

pub struct JournalManager {
    journal_file:     File,
    block_size:       u32,
    first_page:       RawPage,
}

impl JournalManager {

    pub fn open(path: &str) -> std::io::Result<JournalManager> {
        let mut journal_file = File::create(path)?;
        let meta = journal_file.metadata()?;

        let mut first_page = RawPage::new(0, 4096);

        if meta.len() == 0 {
            journal_header_page::init_header_page(&mut first_page);
        } else {
            first_page.read_from_file(&mut journal_file, 0)?;
        }

        Ok(JournalManager {
            journal_file,
            block_size: 4096,
            first_page,
        })
    }

}

mod journal_header_page {
    use crate::page::{ RawPage, header_page_utils };
    static HEADER_DESP: &str       = "PipeappleDB Journal v0.1";

    pub fn init_header_page(page: &mut RawPage) {
        header_page_utils::set_title(page, HEADER_DESP);
    }

}
