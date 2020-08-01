
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
