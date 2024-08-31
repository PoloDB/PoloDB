from rust_polodb import PyDatabase
from contextlib import contextmanager


class PoloDB:

    def __init__(self, path: str) -> None:
        self._path = path
        self.__rust_db = PyDatabase(self._path)

    def __enter__(self):
        self.__rust_db = PyDatabase(self._path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
