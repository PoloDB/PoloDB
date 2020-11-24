use std::path::Path;
use std::process;
use std::fmt;
use std::fmt::Formatter;
use chrono::offset::Local;
use chrono::DateTime;
use chrono::format::{DelayedFormat, StrftimeItems};
use polodb_core::Database;
use polodb_core::dump::{FullDump, JournalDump, PageDump};

struct FullDumpWrapper<'a>(&'a FullDump);
struct PageDumpWrapper<'a>(&'a PageDump);

pub(crate) fn dump(src_path: &str) {
    if !Path::exists(src_path.as_ref()) {
        println!("database not exist: {}", src_path);
        process::exit(2);
    }
    let mut db = Database::open(src_path).unwrap();
    let dump = db.dump().unwrap();
    println!("{}", FullDumpWrapper(&dump));
}

fn format_datetime(datetime: &DateTime<Local>) -> DelayedFormat<StrftimeItems> {
    datetime.format("%d/%m/%Y %T")
}

impl<'a> fmt::Display for FullDumpWrapper<'a> {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "Path:           {}", self.0.path.to_str().unwrap())?;
        writeln!(f, "Identifier:     {}", self.0.identifier)?;
        writeln!(f, "Version:        {}", self.0.version)?;
        writeln!(f, "Page Size:      {}", self.0.page_size)?;
        writeln!(f, "Meta Page Id:   {}", self.0.meta_pid)?;
        writeln!(f, "Free List Page Id: {}" ,self.0.free_list_pid)?;
        writeln!(f, "Free List Size: {}", self.0.free_list_size)?;
        writeln!(f, "")?;

        let created_datetime: DateTime<Local> = self.0.file_meta.created().unwrap().into();
        writeln!(f, "Created Time:   {}", format_datetime(&created_datetime))?;
        let modified_datetime: DateTime<Local> = self.0.file_meta.modified().unwrap().into();
        writeln!(f, "Modified Time:  {}", format_datetime(&modified_datetime))?;
        let size = self.0.file_meta.len();
        writeln!(f, "Size:           {}", size)?;

        dump_journal(self.0.journal_dump.as_ref(), f)?;

        for page_dump in &self.0.pages {
            let wrapper: PageDumpWrapper = page_dump.into();
            writeln!(f, "{}", wrapper)?;
        }

        Ok(())
    }

}

fn dump_journal(journal_dump: &JournalDump, f: &mut Formatter<'_>) -> fmt::Result {
    writeln!(f, "")?;
    writeln!(f, "Journal Path:           {}", journal_dump.path.to_str().unwrap())?;
    let created_datetime: DateTime<Local> = journal_dump.file_meta.created().unwrap().into();
    writeln!(f, "Journal Created Time:   {}", format_datetime(&created_datetime))?;
    let modified_datetime: DateTime<Local> = journal_dump.file_meta.modified().unwrap().into();
    writeln!(f, "Journal Modified Time:  {}", format_datetime(&modified_datetime))?;

    Ok(())
}

impl<'a> From<&'a FullDump> for FullDumpWrapper<'a> {

    fn from(dump: &'a FullDump) -> Self {
        FullDumpWrapper(dump)
    }

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
                write!(f, "Undefined: {}", pid)?;
            }

            PageDump::BTreePage(_) => {

            }

            PageDump::OverflowDataPage(_) => {

            }

            PageDump::DataPage(_) => {

            }

            PageDump::FreeListPage(_) => {

            }

        }

        Ok(())
    }

}
