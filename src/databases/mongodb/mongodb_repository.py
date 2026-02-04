# src/databases/mongodb/mongodb_repository.py

import logging

from src.databases.mongodb.config import MongoDBConfig
from src.databases.mongodb.client import MongoDBClient


class MongoDBRepository(MongoDBClient):
    """
    Repo = Client + CRUD.
    Benchmark tip: call connect_and_cache() once, then use repo.collection (or get_collection())
    inside the benchmark loop to minimize Python overhead in timed sections.
    """

    def __init__(self, mongodb_config : MongoDBConfig, logger: logging.Logger):
        super().__init__(mongodb_config, logger)
        self.db = None
        self.collection = None

    # -----------------------
    # Connection + caching
    # -----------------------
    def connect_and_cache(self) -> bool:
        """
        Connect and cache db/collection objects for fast use in benchmarks.
        """
        if not self.connect():
            self.logger.error("connect_and_cache(): connect() failed")
            return False

        self.db = self.client[self.database_name]
        self.collection = self.db[self.collection_name]

        # Optional: verify access without counting as "connect" time in your benchmark
        try:
            self.client.admin.command("ping")
        except Exception as e:
            self.logger.exception("MongoDB ping failed after connect: %s", e)
            return False

        self.logger.info("Cached db/collection handles (db=%s, collection=%s)", self.database_name, self.collection_name)
        return True

    def disconnect(self) -> bool:
        # clear cached handles
        self.db = None
        self.collection = None
        return super().disconnect()

    def get_collection(self):
        """
        Use this to grab the collection once BEFORE starting your timer.
        """
        if self.collection is None:
            raise RuntimeError("Collection not cached. Call connect_and_cache() first.")
        return self.collection

    # -----------------------
    # Health
    # -----------------------
    def ping(self) -> bool:
        if self.client is None:
            self.logger.error("ping(): not connected")
            return False
        try:
            self.client.admin.command("ping")
            self.logger.info("Ping OK.")
            return True
        except Exception as e:
            self.logger.exception("Ping error: %s", e)
            return False

    # -----------------------
    # INSERT
    # -----------------------
    def insert_one(self, doc: dict, collection=None) -> bool:
        """
        If you pass collection=repo.collection, this method avoids any lookup overhead.
        """
        if not doc:
            self.logger.error("insert_one(): empty doc")
            return False

        col = collection if collection is not None else self.collection
        if col is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            col.insert_one(doc)
            return True
        except Exception as e:
            self.logger.exception("insert_one() error: %s", e)
            return False

    def insert_many(self, docs: list[dict], ordered: bool = False, collection=None) -> bool:
        if not docs:
            self.logger.error("insert_many(): empty docs")
            return False

        col = collection if collection is not None else self.collection
        if col is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            col.insert_many(docs, ordered=ordered)
            return True
        except Exception as e:
            self.logger.exception("insert_many() error: %s", e)
            return False

    # -----------------------
    # READ
    # -----------------------
    def find_by_query(self, query: dict, collection=None) -> list[dict]:
        if query is None:
            self.logger.error("find_by_query(): query is None")
            return []

        col = collection if collection is not None else self.collection
        if col is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            return list(col.find(query))
        except Exception as e:
            self.logger.exception("find_by_query() error: %s", e)
            return []

    def aggregate(self, pipeline: list[dict], collection=None) -> list[dict]:
        if not pipeline:
            self.logger.error("aggregate(): empty pipeline")
            return []

        col = collection if collection is not None else self.collection
        if col is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            return list(col.aggregate(pipeline))
        except Exception as e:
            self.logger.exception("aggregate() error: %s", e)
            return []

    # -----------------------
    # UPDATE
    # -----------------------
    def update_one(self, query: dict, update_fields: dict, collection=None) -> bool:
        if query is None or update_fields is None:
            self.logger.error("update_one(): query/update_fields is None")
            return False

        col = collection if collection is not None else self.collection
        if col is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            res = col.update_one(query, {"$set": update_fields})
            self.logger.info("update_one(): modified=%d", res.modified_count)
            return True
        except Exception as e:
            self.logger.exception("update_one() error: %s", e)
            return False

    def update_many(self, query: dict, update_fields: dict, collection=None) -> bool:
        if query is None or update_fields is None:
            self.logger.error("update_many(): query/update_fields is None")
            return False

        col = collection if collection is not None else self.collection
        if col is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            res = col.update_many(query, {"$set": update_fields})
            self.logger.info("update_many(): modified=%d", res.modified_count)
            return True
        except Exception as e:
            self.logger.exception("update_many() error: %s", e)
            return False

    # -----------------------
    # DELETE
    # -----------------------
    def delete_by_query(self, query: dict, collection=None) -> bool:
        if query is None:
            self.logger.error("delete_by_query(): query is None")
            return False

        col = collection if collection is not None else self.collection
        if col is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            res = col.delete_many(query)
            self.logger.info("delete_by_query(): deleted=%d", res.deleted_count)
            return True
        except Exception as e:
            self.logger.exception("delete_by_query() error: %s", e)
            return False

    def drop_collection(self, collection=None) -> bool:
        col = collection if collection is not None else self.collection
        if col is None:
            raise RuntimeError("No collection available. Call connect_and_cache() first.")

        try:
            col.drop()
            self.logger.info("Dropped collection %s", self.collection_name)
            return True
        except Exception as e:
            self.logger.exception("drop_collection() error: %s", e)
            return False