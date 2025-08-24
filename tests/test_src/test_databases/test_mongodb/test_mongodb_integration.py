# tests/test_src/test_databases/test_mongodb/test_mongodb_integration.py

import datetime
from datetime import datetime, timezone
from bson.objectid import ObjectId

import logging
from pprint import pprint
import pytest

from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.databases.mongodb.utils.data_utils import resolve_data

class TestIntegrationMongoDBRepository:
    
    @pytest.fixture
    def prepared_data(self, load_data: list[dict], logger: logging.Logger) -> list[dict]:
        """Copy + resolve sample data so each test can insert its own docs."""
        data = [dict(d) for d in load_data]
        for doc in data:
            resolve_data(doc, logger)
        return data


    @pytest.fixture(autouse=True)
    def clean_collection(self, mongodb_repo: MongoDBRepository):
        """Ensure collection is empty before and after each test."""
        mongodb_repo.delete_by_query({})
        yield
        mongodb_repo.delete_by_query({})

    def test_ping(self, logger: logging.Logger, mongodb_repo: MongoDBRepository):
        try:
            logger.info("Testing MongoDB ping...")
            assert mongodb_repo.ping() is True
        except Exception as e:
            logger.error(f"Ping test failed: {e}")
            pytest.fail("Ping test failed")

    def test_delete_by_query(self, logger: logging.Logger, mongodb_repo: MongoDBRepository, prepared_data: list[dict]):
        try:
            logger.info("Testing MongoDB delete by query...")
            # Seed some data
            assert mongodb_repo.insert_many(prepared_data[:2]) is True
            # Delete by query
            assert mongodb_repo.delete_by_query({}) is True
            # Verify empty
            found = mongodb_repo.find_by_query({})
            assert isinstance(found, list)
            assert len(found) == 0

            logger.info("MongoDB delete by query test passed.")
        except Exception as e:
            logger.error(f"Delete by query test failed: {e}")
            pytest.fail("Delete by query test failed")


    def test_insert_one(self, logger: logging.Logger, mongodb_repo: MongoDBRepository, prepared_data: list[dict]):
        try:
            logger.info("Testing MongoDB insert one...")
            doc = prepared_data[0]
            assert mongodb_repo.insert_one(doc) is True

            found = mongodb_repo.find_by_query({"_id": doc["_id"]})
            assert isinstance(found, list)
            assert len(found) == 1
            assert found[0]["_id"] == doc["_id"]

            logger.info("MongoDB insert one test passed.")
        except Exception as e:
            logger.error(f"Insert one test failed: {e}")
            pytest.fail("Insert one test failed")

    def test_insert_many(self, logger: logging.Logger, mongodb_repo: MongoDBRepository, prepared_data: list[dict]):
        try:
            logger.info("Testing MongoDB insert many...")
            batch = prepared_data[1:3]
            assert mongodb_repo.insert_many(batch) is True

            ids = [d["_id"] for d in batch]
            found = mongodb_repo.find_by_query({"_id": {"$in": ids}})
            assert isinstance(found, list)
            assert len(found) == len(batch)

            logger.info("MongoDB insert many test passed.")
        except Exception as e:
            logger.error(f"Insert many test failed: {e}")
            pytest.fail("Insert many test failed")


    def test_find_by_query(self, logger: logging.Logger, mongodb_repo: MongoDBRepository, prepared_data: list[dict]):
        try:
            logger.info("Testing MongoDB find by query...")
            # Seed
            assert mongodb_repo.insert_many(prepared_data[:3]) is True

            # Query
            found = mongodb_repo.find_by_query({"meta.source": "Node 1"})
            assert isinstance(found, list)
            assert len(found) >= 1
            # pprint(found)
            
            logger.info("Find by query test passed.")
        except Exception as e:
            logger.error(f"Find by query test failed: {e}")
            pytest.fail("Find by query test failed")


    def test_aggregate(self, logger: logging.Logger, mongodb_repo: MongoDBRepository, prepared_data: list[dict]):
        try:
            logger.info("Testing MongoDB aggregate...")
            # Seed
            assert mongodb_repo.insert_many(prepared_data[:4]) is True

            # Aggregate
            pipeline = [
                {"$match": {"meta.source": "Node 1"}},
                {"$project": {"time": 1, "_id": 1}},
            ]
            result = mongodb_repo.aggregate(pipeline)
            assert isinstance(result, list)
            assert all("_id" in d for d in result)
            assert all("time" in d for d in result)
            # pprint(result)

            logger.info("MongoDB aggregate test passed.")
        except Exception as e:
            logger.error(f"MongoDB aggregate test failed: {e}")
            pytest.fail("MongoDB aggregate test failed")


    def test_update_single_doc_via_meta_filter(self, logger: logging.Logger, mongodb_repo: MongoDBRepository, prepared_data: list[dict]):
        try:
            logger.info("Insert 1 doc, fetch it, then update via meta-only (time-series safe)")

            # 1) Insert exactly one document
            doc = prepared_data[0]
            assert mongodb_repo.insert_one(doc) is True

            # 2) Fetch it back (you asked to use find_by_query)
            found = mongodb_repo.find_by_query({})
            assert isinstance(found, list) and len(found) == 1

            # 3) Grab its _id (for sanity/logging) and meta.source_id (for legal TS update)
            inserted_id = found[0]["_id"]
            source_id = found[0]["meta"]["source_id"]
            logger.info(f"Inserted _id={inserted_id}, source_id={source_id}")

            # 4) Update using a meta-only filter and meta-only update (required by TS)
            flt = {"meta.source_id": source_id}               # ✅ meta-only filter
            upd = {"meta.source": "Node X"}         # ✅ meta-only update
            assert mongodb_repo.update_many(flt, upd) is True

            # 5) Verify the same document (matched by meta) now has updated meta.source
            after = mongodb_repo.find_by_query({"meta.source_id": source_id})
            assert isinstance(after, list) and len(after) == 1
            assert after[0]["_id"] == inserted_id            # same document
            assert after[0]["meta"]["source"] == "Node X"    # updated value
        except Exception as e:
            logger.error(f"Update test failed: {e}")
            pytest.fail("Update test failed")