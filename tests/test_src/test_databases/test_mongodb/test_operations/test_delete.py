# tests/test_src/test_databases/test_mongodb/test_operations/test_delete.py

from unittest.mock import MagicMock, patch
from src.databases.mongodb.operations.delete import DeleteOperations

class TestMongoDBOperationsDelete:

    @patch.object(DeleteOperations, 'delete_by_query')
    def test_delete_by_query(self, delete_by_query_mock: MagicMock):
        # Arrange
        delete_operations = DeleteOperations(mongodb_client=MagicMock())
        query = {"name": "test"}
        delete_by_query_mock.return_value = True

        # Act
        result = delete_operations.delete_by_query(query)

        # Assert
        assert result is True
        delete_by_query_mock.assert_called_once_with(query)