/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use csv::{Reader, StringRecord};
use polodb_core::LsmKv;
use polodb_core::test_utils::mk_db_path;

#[test]
fn test_insert_db() {
    vec![
        LsmKv::open_file(mk_db_path("test-insert-kv").as_path()).unwrap(),
        LsmKv::open_memory().unwrap(),
    ].iter().for_each(|db| {
        db.put("Hello", "World").unwrap();
        db.put(vec![1u8], vec![2u8]).unwrap();

        let value = db.get_string("Hello").unwrap().unwrap();
        assert_eq!(value, "World");

        let value = db.get(&[1u8]).unwrap().unwrap();
        assert_eq!(value.as_ref(), &[2u8]);
    });
}

fn clean_path(path: &Path) {
    let str = path.to_str().unwrap().to_string();
    let str_wal = str + ".wal";

    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(PathBuf::from(str_wal));
}

#[test]
fn test_persist() {
    let db_path = mk_db_path("test-kv-persist");
    clean_path(db_path.as_path());
    {
        let db = LsmKv::open_file(db_path.as_path()).unwrap();
        db.put("Hello", "World").unwrap();
        db.put("name", "Vincent").unwrap();
    }

    {
        let db = LsmKv::open_file(db_path.as_path()).unwrap();
        let value = db.get_string("Hello").unwrap().unwrap();
        assert_eq!(value, "World");

        let value = db.get_string("name").unwrap().unwrap();
        assert_eq!(value, "Vincent");
    }
}

#[test]
fn test_order() {
    vec![
        LsmKv::open_file(mk_db_path("test-kv-order").as_path()).unwrap(),
        LsmKv::open_memory().unwrap(),
    ].iter().for_each(|db| {
        for i in 0..10 {
            db.put(vec![i as u8], vec![i as u8]).unwrap();
        }

        let cursor = db.open_cursor();
        cursor.seek(vec![5 as u8]).unwrap();
        assert_eq!(cursor.value().unwrap().unwrap()[0], 5);

        cursor.next().unwrap();
        assert_eq!(cursor.value().unwrap().unwrap()[0], 6);
    });
}

#[test]
fn test_delete() {
    vec![
        LsmKv::open_file(mk_db_path("test-kv-delete").as_path()).unwrap(),
        LsmKv::open_memory().unwrap(),
    ].iter().for_each(|db| {
        for i in 0..10 {
            db.put(vec![i as u8], vec![i as u8]).unwrap();
        }

        db.delete(&[5u8]).unwrap();

        let cursor = db.open_cursor();
        cursor.seek(vec![4 as u8]).unwrap();

        assert_eq!(cursor.value().unwrap().unwrap()[0], 4);

        cursor.next().unwrap();
        assert_eq!(cursor.value().unwrap().unwrap()[0], 6);
    });
}

fn find_crime_desc(header: &StringRecord) -> usize {
    let mut index: usize = 0;
    for h in header {
        if h == "Crm Cd Desc" {
            break;
        }
        index += 1;
    }

    index
}

fn insert_csv_to_db(db: &LsmKv, rdr: &mut Reader<&File>, mem_table: &mut HashMap<String, String>, count: usize) {
    let header = rdr.headers().unwrap();
    let index: usize = find_crime_desc(header);

    let mut counter: usize = 0;
    for result in rdr.records() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let record = result.unwrap();

        let key = record.get(0).unwrap();
        let content = record.get(index).unwrap();

        db.put(key, content).unwrap();

        mem_table.insert(key.into(), content.into());

        counter += 1;
        if counter >= count {
            break;
        }
    }
}

/// insert 1500
#[test]
fn test_dataset_1500() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let data_set_path = dir.to_string() + "/tests/dataset/CrimeDataFrom2020.csv";
    let file = File::open(data_set_path).unwrap();

    let db_path = mk_db_path("test-kv-dataset-1500");
    clean_path(db_path.as_path());

    let mut mem_table: HashMap<String, String> = HashMap::new();

    {
        let db = LsmKv::open_file(db_path.as_path()).unwrap();
        let metrics = db.metrics();
        metrics.enable();

        let mut rdr = Reader::from_reader(&file);

        insert_csv_to_db(&db, &mut rdr, &mut mem_table, 1500);

        assert_eq!(metrics.sync_count(), 1);
    }

    let db = LsmKv::open_file(db_path.as_path()).unwrap();

    // in sstable
    let test0 = db.get_string("200100509").unwrap().unwrap();
    assert_eq!(test0, "BURGLARY FROM VEHICLE");

    // in log
    let test1 = db.get_string("201108111").unwrap().unwrap();
    assert_eq!(test1, "BATTERY - SIMPLE ASSAULT");

    let mut counter = 0;
    for (key, value) in &mem_table {
        let test_value = String::from_utf8(
        db.get(key.as_str())
                .unwrap()
                .expect(format!("no value: {}, key: {}", counter, key).as_str())
                .to_vec()
        ).unwrap();
        assert_eq!(test_value.as_str(), value.as_str(), "key: {}, counter: {}", key, counter);
        counter += 1;
    }
}

/// insert 3500
#[test]
fn test_dataset_3500() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let data_set_path = dir.to_string() + "/tests/dataset/CrimeDataFrom2020.csv";
    let file = std::fs::File::open(data_set_path).unwrap();

    let db_path = mk_db_path("test-kv-dataset-3500");
    clean_path(db_path.as_path());

    let mut mem_table: HashMap<String, String> = HashMap::new();

    {
        let db = LsmKv::open_file(db_path.as_path()).unwrap();
        let metrics = db.metrics();
        metrics.enable();

        let mut rdr = Reader::from_reader(&file);

        insert_csv_to_db(&db, &mut rdr, &mut mem_table, 3500);

        assert_eq!(metrics.sync_count(), 3);
    }

    let db = LsmKv::open_file(db_path.as_path()).unwrap();

    // in sstable
    let test0 = db.get_string("200100509").unwrap().unwrap();
    assert_eq!(test0, "BURGLARY FROM VEHICLE");

    // in log
    let test1 = db.get_string("201108111").unwrap().unwrap();
    assert_eq!(test1, "BATTERY - SIMPLE ASSAULT");

    let mut counter = 0;
    for (key, value) in &mem_table {
        let test_value = db.get_string(key.as_str())
            .unwrap()
            .expect(format!("no value: {}, key: {}", counter, key).as_str());
        assert_eq!(test_value.as_str(), value.as_str(), "key: {}, counter: {}", key, counter);
        counter += 1;
    }
}

/// insert 7500
#[test]
fn test_dataset_7500() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let data_set_path = dir.to_string() + "/tests/dataset/CrimeDataFrom2020.csv";
    let file = std::fs::File::open(data_set_path).unwrap();

    let db_path = mk_db_path("test-kv-dataset-7500");
    clean_path(db_path.as_path());

    let mut mem_table: HashMap<String, String> = HashMap::new();

    {
        let db = LsmKv::open_file(db_path.as_path()).unwrap();
        let metrics = db.metrics();
        metrics.enable();

        let mut rdr = Reader::from_reader(&file);

        insert_csv_to_db(&db, &mut rdr, &mut mem_table, 7500);

        // assert_eq!(metrics.sync_count(), 7);
        assert_eq!(metrics.minor_compact(), 1);
    }

    let db = LsmKv::open_file(db_path.as_path()).unwrap();
    let metrics = db.metrics();

    assert_eq!(1, metrics.free_segments_count());

    // in sstable
    let test0 = db.get_string("200100509").unwrap().unwrap();
    assert_eq!(test0, "BURGLARY FROM VEHICLE");

    // in log
    let test1 = db.get_string("201108111").unwrap().unwrap();
    assert_eq!(test1, "BATTERY - SIMPLE ASSAULT");

    let mut counter = 0;
    for (key, value) in &mem_table {
        let test_value = db.get_string(key.as_str())
            .unwrap()
            .expect(format!("no value: {}, key: {}", counter, key).as_str());
        assert_eq!(test_value.as_str(), value.as_str(), "key: {}, counter: {}", key, counter);
        counter += 1;
    }
}

/// insert 16k data
/// including major compact
#[test]
fn test_dataset_18k() {
    let dir = env!("CARGO_MANIFEST_DIR");
    let data_set_path = dir.to_string() + "/tests/dataset/CrimeDataFrom2020.csv";
    let file = File::open(data_set_path).unwrap();

    let db_path = mk_db_path("test-kv-dataset-18k");
    println!("18k data: {}", db_path.to_str().unwrap());
    clean_path(db_path.as_path());

    let mut mem_table: HashMap<String, String> = HashMap::new();

    {
        let db = LsmKv::open_file(db_path.as_path()).unwrap();
        let metrics = db.metrics();
        metrics.enable();

        let mut rdr = Reader::from_reader(&file);

        insert_csv_to_db(&db, &mut rdr, &mut mem_table, 18000);

        assert_eq!(metrics.minor_compact(), 4);
        assert_eq!(metrics.major_compact(), 1);

        assert_eq!(metrics.use_free_segment_count(), 11);
    }

    let db = LsmKv::open_file(db_path.as_path()).unwrap();

    // in sstable
    let test0 = db.get_string("200100509").unwrap().unwrap();
    assert_eq!(test0, "BURGLARY FROM VEHICLE");

    // in log
    let test1 = db.get_string("201108111").unwrap().unwrap();
    assert_eq!(test1, "BATTERY - SIMPLE ASSAULT");

    let mut counter = 0;
    for (key, value) in &mem_table {
        let test_value = db.get_string(key.as_str())
            .unwrap()
            .expect(format!("no value: {}, key: {}", counter, key).as_str());
        assert_eq!(test_value.as_str(), value.as_str(), "key: {}, counter: {}", key, counter);
        counter += 1;
    }
}
