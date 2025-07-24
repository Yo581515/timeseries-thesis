# tests/test_src/test_databases/test_mongodb/test_client.py

from unittest.mock import patch
import logging

from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.config import MongoDBConfig


class TestMongoDBClient:

    def get_mock_config(self):
        return MongoDBConfig(
            username="test_user",
            password="test_pass",
            cluster="test-cluster.mongodb.net",
            database="test_db",
            collection="test_collection"
        )

    def get_mock_logger(self):
        logger = logging.getLogger("mock_logger")
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        return logger

    @patch.object(MongoDBClient, 'connect')
    def test_connect_mocked(self, mock_connect):
        mock_connect.return_value = True

        config = self.get_mock_config()
        logger = self.get_mock_logger()

        client = MongoDBClient(config, logger)
        result = client.connect()
        assert result is True

    @patch.object(MongoDBClient, 'disconnect')
    def test_disconnect_mocked(self, mock_disconnect):
        mock_disconnect.return_value = None

        config = self.get_mock_config()
        logger = self.get_mock_logger()

        client = MongoDBClient(config, logger)
        result = client.disconnect()
        assert result is None
