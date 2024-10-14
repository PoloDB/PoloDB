import pytest
from polodb import PoloDB

import os
import shutil

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


TEST_DATA_DIR = BASE_DIR / "data"
TEST_DATA_PATH = TEST_DATA_DIR / "dbtest"
TEST_COLLECTION_NAME = "test_collection"

os.path.exists(TEST_DATA_DIR.absolute()) or os.makedirs(TEST_DATA_DIR.absolute())


@pytest.fixture(scope="module")
def data_path():
    return TEST_DATA_PATH.as_posix()


@pytest.fixture(scope="module")
def collection_name():
    return TEST_COLLECTION_NAME


@pytest.fixture(scope="module")
def db(data_path):
    yield PoloDB(data_path)
    shutil.rmtree(data_path)
