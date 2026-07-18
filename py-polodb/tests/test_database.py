import pytest

from polodb import PoloDB, Collection


def test_db_initialization(db, data_path):
    assert isinstance(db, PoloDB)
    assert db._path == data_path


def test_db_collection_creation(db, collection_name):
    collection = db.collection(collection_name)
    assert isinstance(collection, Collection)
    assert collection.name() == collection_name
    assert db[collection_name].name() == collection_name


def test_db_list_collection_names(db, collection_name):
    db.collection(collection_name)
    collections = db.list_collection_names()
    assert collection_name in collections


def test_insert_one_round_trip_and_missing_result(db):
    collection = db.collection("test_collection")
    entry = {
        "name": "Alice",
        "active": True,
        "age": 30,
        "score": 9.5,
        "profile": {"city": "Paris"},
        "tags": ["admin", 7],
    }
    insert_result = collection.insert_one(entry)
    assert set(insert_result) == {"inserted_id"}

    found_entry = collection.find_one({"name": "Alice"})
    for key, value in entry.items():
        assert found_entry[key] == value

    assert collection.find_one({"name": "missing"}) is None


def test_insert_many_find_and_count(db):
    collection = db.collection("test_collection")
    insert_result = collection.insert_many(
        [
            {"name": "Alice", "group": "staff"},
            {"name": "Bob", "group": "staff"},
            {"name": "Carol", "group": "guest"},
        ]
    )

    assert set(insert_result) == {0, 1, 2}
    assert collection.len() == 3
    assert {item["name"] for item in collection.find({"group": "staff"})} == {
        "Alice",
        "Bob",
    }


def test_update_results_and_persisted_values(db):
    collection = db.collection("test_collection")
    collection.insert_many(
        [
            {"name": "Alice", "group": "staff", "active": False},
            {"name": "Bob", "group": "staff", "active": False},
            {"name": "Carol", "group": "guest", "active": False},
        ]
    )

    update_one = collection.update_one(
        {"name": "Alice"}, {"$set": {"active": True}}
    )
    assert update_one == {"matched_count": 1, "modified_count": 1}
    assert collection.find_one({"name": "Alice"})["active"] is True

    update_many = collection.update_many(
        {"group": "staff"}, {"$set": {"role": "member"}}
    )
    assert update_many == {"matched_count": 2, "modified_count": 2}
    assert len(collection.find({"role": "member"})) == 2


def test_aggregate_and_delete_results(db):
    collection = db.collection("test_collection")
    collection.insert_many(
        [
            {"name": "banana", "color": "yellow"},
            {"name": "pear", "color": "yellow"},
            {"name": "apple", "color": "red"},
        ]
    )

    aggregate_result = collection.aggregate(
        [{"$match": {"color": "yellow"}}, {"$count": "count"}]
    )
    assert aggregate_result == [{"count": 2}]

    assert collection.delete_one({"color": "yellow"}) == {"deleted_count": 1}
    assert collection.delete_many({"color": "yellow"}) == {"deleted_count": 1}
    assert collection.len() == 1


def test_invalid_python_value_returns_exception(db):
    collection = db.collection("test_collection")

    with pytest.raises(RuntimeError, match="Unsupported Python type"):
        collection.insert_one({"unsupported": object()})
