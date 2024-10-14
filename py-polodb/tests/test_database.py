from polodb import PoloDB, Collection


def test_db_initialization(db, data_path):
    assert isinstance(db, PoloDB)
    assert db._path == data_path


def test_db_collection_creation(db, collection_name):
    collection = db.collection(collection_name)
    assert isinstance(collection, Collection)
    assert collection.name() == collection_name


def test_db_list_collection_names(db, collection_name):
    db.collection(collection_name)
    collections = db.list_collection_names()
    assert collection_name in collections


def test_db_insert_and_find_one(db):
    # Test inserting and finding a document
    collection = db.collection("test_collection")
    entry = {"name": "Alice", "age": 30}
    insert_result = collection.insert_one(entry)
    assert insert_result is not None
    found_entry = collection.find_one({"name": "Alice"})
    assert found_entry["name"] == "Alice"
    assert found_entry["age"] == 30
