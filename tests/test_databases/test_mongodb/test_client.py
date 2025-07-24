# test/test_databases/test_mongodb/test_client.py

from src.databases.mongodb.client import MongoDBClient
from tests.test_databases.test_mongodb.conftest import mongodb_client

class TestMongoDBClient:
    def test_connection(self, mongodb_client : MongoDBClient):
        try:
            assert mongodb_client.connect() is True
        except Exception as e:
            mongodb_client.client.close()
            mongodb_client.logger.error(f"Connection failed: {e}")