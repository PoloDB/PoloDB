import pytest
from polodb import PoloDB

TEST_COLLECTION_NAME = "test_collection"


@pytest.fixture
def data_path(tmp_path):
    return (tmp_path / "dbtest").as_posix()


@pytest.fixture
def collection_name():
    return TEST_COLLECTION_NAME


@pytest.fixture
def db(data_path):
    return PoloDB(data_path)
