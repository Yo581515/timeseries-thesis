# tests/test_src/test_databases/test_mongodb/test_utils/test_data_utils.py

import datetime
import logging
import unittest

from src.databases.mongodb.utils.data_utils import convert_time, convert_location, extract_meta_fields, resolve_data

class TestMDDataUtils(unittest.TestCase):
    def get_mock_logger(self):
        logger = logging.getLogger("mock_logger")
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        return logger

    def test_convert_time(self):
        logger = self.get_mock_logger()
        data_point = {"time": "2024-09-18T20:06:45.179+00:00"}
        assert convert_time(data_point, logger) is True
        assert isinstance(data_point['time'], datetime.datetime)
        assert data_point['time'].tzinfo is not None  # Check if timezone is set
        

    def test_convert_location(self):
        logger = self.get_mock_logger()
        data_point = {"location": {"latitude": 40.7128, "longitude": -74.0060}}
        assert convert_location(data_point, logger) is True
        assert data_point['location']['type'] == "Point"
        assert data_point['location']['coordinates'] == [-74.0060, 40.7128]

    def test_extract_meta_fields(self):
        logger = self.get_mock_logger()
        data_point = {
            "source": "Node 1",
            "source_id": "12345",
            "location": {
                "coordinates": [-74.0060, 40.7128],
                "type": "Point"
            }
        }
        assert extract_meta_fields(data_point, logger) is True
        assert 'meta' in data_point
        assert data_point['meta']['source'] == "Node 1"
        assert data_point['meta']['source_id'] == "12345"
        assert data_point['meta']['location']['type'] == "Point"
        assert data_point['meta']['location']['coordinates'] == [-74.0060, 40.7128]

    def test_resolve_data(self):
        logger = self.get_mock_logger()
        data_point = {
            "time": "2024-09-18T20:06:45.179+00:00",
            "location": {"latitude": 40.7128, "longitude": -74.0060},
            "source": "Node 1",
            "source_id": "12345"
        }
        resolved_data = resolve_data(data_point, logger)
        assert isinstance(resolved_data['time'], datetime.datetime)
        assert resolved_data['meta']['location']['type'] == "Point"
        assert resolved_data['meta']['location']['coordinates'] == [-74.0060, 40.7128]
        assert 'meta' in resolved_data
        assert resolved_data['meta']['source'] == "Node 1"
        assert resolved_data['meta']['source_id'] == "12345"