use std::fs::File;
use std::sync::Arc;
use super::page::{ RawPage, HeaderPage };

static HEADER_DESP: &str = "PipeappleDB Format v0.1";

pub struct DbContext {
    db_file:    File,
    block_size: u32,
    first_page: HeaderPage,
}

pub struct Database {
    ctx: Arc<DbContext>,
}

fn check_and_write_first_block(file: &mut File, block_size: u32) -> std::io::Result<RawPage> {
    let meta = file.metadata()?;
    let file_len = meta.len();
    if file_len < block_size as u64 {
        force_write_first_block(file, block_size)
    } else {
        read_first_block(file, block_size)
    }
}

/**
 * Offset 0 (32 bytes) : "PipeappleDB Format v0.1";
 * Offset 32 (8 bytes) : Version 0.0.0.0;
 * Offset 40 (4 bytes) : SectorSize;
 * Offset 44 (4 bytes) : PageSize;
 *
 * Free list offset: 2048;
 * | 4b   | 4b                  | 4b     | 4b    | ... |
 * | size | free list page link | free 1 | free2 | ... |
 */
fn force_write_first_block(file: &mut File, block_size: u32) -> std::io::Result<RawPage> {
    let mut raw_page = RawPage::new(block_size as usize);
    raw_page.put_str(HEADER_DESP).expect("space not enough");
    raw_page.sync_to_file(file, 0)?;
    Ok(raw_page)
}

fn read_first_block(file: &mut File, block_size: u32) -> std::io::Result<RawPage> {
    let mut raw_page = RawPage::new(block_size as usize);
    raw_page.read_from_file(file, 0)?;
    Ok(raw_page)
}

impl DbContext {

    fn new(path: String) -> std::io::Result<DbContext> {
        let mut file = File::create(path)?;
        let block_size = 4096;
        let first_raw_page = check_and_write_first_block(&mut file, block_size)?;
        let first_page = HeaderPage::from_raw(&first_raw_page).expect("parse first page error");
        let mut ctx = DbContext {
            db_file: file,
            block_size,
            first_page,
        };
        return Ok(ctx);
    }

}

impl Database {

    fn new(path: String) -> std::io::Result<Database>  {
        let ctx = DbContext::new(path)?;
        let rc_ctx: Arc<DbContext> = Arc::new(ctx);
        return Ok(Database {
            ctx: rc_ctx,
        });
    }

}
