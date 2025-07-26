# tests/test_src/test_databases/test_mongodb/test_mongodb_integration_test.py

import logging
from pprint import pprint
import pytest

from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.databases.mongodb.utils.data_utils import resolve_data

class TestMongoDBIntegration:
    
    @pytest.mark.test_mongodb_integration
    def test_mongodb_integration(self, logger: logging.Logger, mongodb_repo: MongoDBRepository, load_data):
        assert mongodb_repo.ping() is True

        assert mongodb_repo.delete_by_query({}) is True

        data = load_data

        for i, doc in enumerate(data):
            resolve_data(doc, logger)
            
        print("Resolved data:")
        pprint(data[0])
            
        # Insert one
        assert mongodb_repo.insert_one(data[0]) is True

        # Insert many
        assert mongodb_repo.insert_many(data[1:3]) is True

        # Find by query
        found = mongodb_repo.find_by_query({"meta.source": "Node 1"})
        assert isinstance(found, list)
        assert len(found) >= 1
        pprint(found)

        # Aggregate
        pipeline = [
            {"$match": {"meta.source": "Node 1"}},
            {"$project": {"time": 1, "_id": 1}}
        ]
        result = mongodb_repo.aggregate(pipeline)
        assert isinstance(result, list)
        pprint(result)

        # Cleanup
        #assert mongodb_repo.delete_by_query({}) is True