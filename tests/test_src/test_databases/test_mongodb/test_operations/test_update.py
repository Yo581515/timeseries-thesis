# tests/test_src/test_databases/test_mongodb/test_operations/test_update.py

from unittest.mock import MagicMock, patch
from src.databases.mongodb.operations.update import UpdateOperations

class TestMongoDBOperationsUpdate:
    
    @patch.object(UpdateOperations, 'update_one')
    def test_update_one(self, update_one_mock: MagicMock):
        # Arrange
        update_operations = UpdateOperations(mongodb_client=MagicMock())
        query = {"name": "test"}
        update = {"$set": {"age": 30}}
        update_one_mock.return_value = True

        # Act
        result = update_operations.update_one(query, update)

        # Assert
        assert result is True
        update_one_mock.assert_called_once_with(query, update)