use polodb_core::LsmKv;
use polodb_core::test_utils::mk_db_path;

#[test]
fn test_insert_db() {
    let db_path = mk_db_path("test-insert-kv");
    let db = LsmKv::open_file(&db_path).unwrap();

    db.put("Hello", "World").unwrap();
    db.put(vec![1u8], vec![2u8]).unwrap();

    let value = db.get_string("Hello").unwrap().unwrap();
    assert_eq!(value, "World");

    let value = db.get(&[1u8]).unwrap().unwrap();
    assert_eq!(value.as_slice(), &[2u8]);
}

#[test]
fn test_order() {
    let db_path = mk_db_path("test-kv-order");
    let db = LsmKv::open_file(&db_path).unwrap();

    for i in 0..10 {
        db.put(vec![i as u8], vec![i as u8]).unwrap();
    }

    let cursor = db.open_cursor();
    cursor.seek(vec![5 as u8]).unwrap();
    assert_eq!(cursor.value().unwrap().unwrap()[0], 5);

    cursor.next().unwrap();
    assert_eq!(cursor.value().unwrap().unwrap()[0], 6);
}
