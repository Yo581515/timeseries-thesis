# tests/test_src/test_databases/test_mongodb/test_client.py

from unittest.mock import MagicMock, patch

from src.databases.mongodb.client import MongoDBClient


class TestMongoDBClient:
    
    @patch.object(MongoDBClient, 'connect')
    def test_connect_mocked(self, mock_connect : MagicMock):
        # Arrange
        mock_connect.return_value = True

        # Act
        client = MongoDBClient(mongodb_config=MagicMock(), logger=MagicMock())
        result = client.connect()

        # Assert
        assert result is True
        mock_connect.assert_called_once()

    @patch.object(MongoDBClient, 'disconnect')
    def test_disconnect_mocked(self, mock_disconnect : MagicMock):

        # Arrange
        mock_disconnect.return_value = True

        # Act
        client = MongoDBClient(mongodb_config=MagicMock(), logger=MagicMock())
        result = client.disconnect()

        # Assert
        assert result is True
        mock_disconnect.assert_called_once()
