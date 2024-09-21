from rust_polodb import PyDatabase, PyCollection


class PoloDB:

    def __init__(self, path: str) -> None:
        self._path = path
        self._rust_db = PyDatabase(self._path)

    def __enter__(self):
        self._rust_db = PyDatabase(self._path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __getitem__(self, name):
        return self.collection(name)

    def __getattr__(self, name: str):
        return self.__getitem__(name)

    def collection(self, name):
        if name not in self.list_collection_names():
            self._rust_db.create_collection(name)
        return Collection(self, name)

    def list_collection_names(self):
        return self._rust_db.list_collection_names()


class Collection:
    def __init__(self, db: PoloDB, name) -> None:
        self.db = db
        self.rust_collection: PyCollection = db._rust_db.collection(name)

    def name(self):
        return self.rust_collection.name()

    def insert_one(self, entry: dict):
        return self.rust_collection.insert_one(entry)

    def find_one(self, filter: dict):
        return self.rust_collection.find_one(filter)
