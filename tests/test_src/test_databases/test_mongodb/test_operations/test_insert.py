# tests/test_src/test_databases/test_mongodb/test_operations/test_insert.py

from unittest.mock import MagicMock, patch

from src.databases.mongodb.operations.insert import InsertOperations

class TestMongoDBOperationsInsert:

    @patch.object(InsertOperations, 'insert_one')
    def test_insert_one(self, insert_one_mock: MagicMock):
        # Arrange
        insert_operations = InsertOperations(mongodb_client=MagicMock())
        document = {"name": "test", "value": 42}
        insert_one_mock.return_value = True

        # Act
        result = insert_operations.insert_one(document)

        # Assert
        assert result is True
        insert_one_mock.assert_called_once_with(document)
        
        
    @patch.object(InsertOperations, 'insert_many')
    def test_insert_many(self, insert_many_mock: MagicMock):
        # Arrange
        insert_operations = InsertOperations(mongodb_client=MagicMock())
        documents = [{"name": "test1", "value": 42}, {"name": "test2", "value": 43}]
        insert_many_mock.return_value = True

        # Act
        result = insert_operations.insert_many(documents)

        # Assert
        assert result is True
        insert_many_mock.assert_called_once_with(documents)