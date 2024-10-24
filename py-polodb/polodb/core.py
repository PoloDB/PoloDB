from rust_polodb import PyDatabase, PyCollection


class PoloDB:

    def __init__(self, path: str) -> None:
        self._path = path
        self.__rust_db = PyDatabase(self._path)

    def __enter__(self):
        self.__rust_db = PyDatabase(self._path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __getitem__(self, name):
        return self.collection(name)

    def __getattr__(self, name: str):
        return self.__getitem__(name)

    def collection(self, name):
        if name not in self.list_collection_names():
            self.__rust_db.create_collection(name)
        return Collection(self.__rust_db.collection(name))

    def list_collection_names(self):
        return self.__rust_db.list_collection_names()


class Collection:
    def __init__(self, rust_collection) -> None:
        self.__rust_collection: PyCollection = rust_collection

    def name(self):
        return self.__rust_collection.name()

    def insert_one(self, entry: dict):
        return self.__rust_collection.insert_one(entry)

    def insert_many(self, entry: dict):
        return self.__rust_collection.insert_many(entry)

    def find_one(self, filter: dict):
        return self.__rust_collection.find_one(filter)

    def find(self, filter: dict):
        return self.__rust_collection.find(filter)

    def update_many(self, filter: dict, update_doc: dict):
        return self.__rust_collection.update_many(filter, update_doc)

    def update_one(self, filter: dict, update_doc: dict):
        return self.__rust_collection.update_one(filter, update_doc)

    def delete_many(self, filter: dict):
        return self.__rust_collection.delete_many(filter)

    def delete_one(self, filter: dict):
        return self.__rust_collection.delete_one(filter)

    def len(self):
        return self.__rust_collection.count_documents()
