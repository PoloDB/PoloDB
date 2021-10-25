use std::path::Path;
use std::process;
use std::fmt;
use std::fmt::Formatter;
use chrono::offset::Local;
use chrono::DateTime;
use chrono::format::{DelayedFormat, StrftimeItems};
use polodb_core::Database;
use polodb_core::dump::{FullDump, JournalDump, PageDump};

struct FullDumpWrapper<'a> {
    dump: &'a FullDump,
    print_page_detail: bool,
}
struct PageDumpWrapper<'a>(&'a PageDump);

pub(crate) fn dump(src_path: &str, page_detail: bool) {
    if !Path::exists(src_path.as_ref()) {
        println!("database not exist: {}", src_path);
        process::exit(2);
    }
    let mut db = Database::open(src_path).unwrap();
    let dump = db.dump().unwrap();
    println!("{}", FullDumpWrapper{ dump: &dump, print_page_detail: page_detail });
}

fn format_datetime(datetime: &DateTime<Local>) -> DelayedFormat<StrftimeItems> {
    datetime.format("%Y/%m/%d %T")
}

macro_rules! write_kv {
    ($formatter:expr, $key:expr, $value:expr) => {
        writeln!($formatter, "{:24}{}", concat!($key, ":"), $value)
    }
}

impl<'a> fmt::Display for FullDumpWrapper<'a> {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_kv!(f, "Path", self.dump.path.to_str().unwrap())?;
        write_kv!(f, "Identifier", self.dump.identifier)?;
        write_kv!(f, "Version", self.dump.version)?;
        write_kv!(f, "Page Size", self.dump.page_size)?;
        write_kv!(f, "Meta Page Id", self.dump.meta_pid)?;
        write_kv!(f, "Free List Page Id" ,self.dump.free_list_pid)?;
        write_kv!(f, "Free List Size", self.dump.free_list_size)?;

        if self.print_page_detail {
            for page_dump in &self.dump.pages {
                let wrapper: PageDumpWrapper = page_dump.into();
                writeln!(f, "{}", wrapper)?;
            }
        }

        dump_journal(self.dump.journal_dump.as_ref(), f)?;

        Ok(())
    }

}

fn dump_journal(journal_dump: &JournalDump, f: &mut Formatter<'_>) -> fmt::Result {
    writeln!(f)?;
    write_kv!(f, "Journal Path", journal_dump.path.to_str().unwrap())?;
    write_kv!(f, "Frame Count", journal_dump.frame_count)?;
    write_kv!(f, "Size", journal_dump.file_meta.len())?;
    let created_datetime: DateTime<Local> = journal_dump.file_meta.created().unwrap().into();
    write_kv!(f, "Created Time", format_datetime(&created_datetime))?;
    let modified_datetime: DateTime<Local> = journal_dump.file_meta.modified().unwrap().into();
    write_kv!(f, "Modified Time", format_datetime(&modified_datetime))?;

    Ok(())
}

impl<'a> From<&'a PageDump> for PageDumpWrapper<'a> {

    fn from(dump: &'a PageDump) -> Self {
        PageDumpWrapper(dump)
    }

}

impl<'a> fmt::Display for PageDumpWrapper<'a> {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            PageDump::Undefined(pid) => {
                writeln!(f, "Undefined:          {}", pid)?;
            }

            PageDump::BTreePage(dump) => {
                writeln!(f, "BTreePage:          {}", dump.pid)?;
                writeln!(f, "Node Size:          {}", dump.node_size)?
            }

            PageDump::OverflowDataPage(_) => {
                unimplemented!();
            }

            PageDump::DataPage(dump) => {
                writeln!(f, "Data Page:          {}", dump.pid)?;
            }

            PageDump::FreeListPage(dump) => {
                writeln!(f, "FreeList:           {}", dump.pid)?;
                writeln!(f, "Size:               {}", dump.size)?;
                writeln!(f, "Next Page Id:       {}", dump.next_pid)?;
            }

        }

        Ok(())
    }

}
