# test/test_databases/test_mongodb/test_utils/test_data_utils.py

import datetime
from src.databases.mongodb.utils.data_utils import convert_time, convert_location, extract_meta_fields, resolve_data

class TestMDDataUtils:
    passed = True
    # def test_convert_time(self, logger):
    #     data_point = {'time': '2024-09-18T20:06:45.179+00:00'}
    #     assert convert_time(data_point, logger) is True
    #     date_time_obj = datetime.datetime.fromisoformat('2024-09-18T20:06:45.179+00:00')
    #     assert data_point['time'] == date_time_obj

    # def test_convert_location(self, logger):
    #     data_point = {'location': {'latitude': 40.7128, 'longitude': -74.0060}}
    #     assert convert_location(data_point, logger) is True
    #     assert data_point['location']['type'] == 'Point'
    #     assert data_point['location']['coordinates'] == [-74.0060, 40.7128]

    # def test_extract_meta_fields(self, logger):
    #     data_point = {
    #         'source': 'sensor1',
    #         'source_id': '12345',
    #         'location': {'latitude': 40.7128, 'longitude': -74.0060},
    #         'value': 42
    #     }
    #     assert extract_meta_fields(data_point, logger) is True
    #     assert 'meta' in data_point
    #     assert data_point['meta']['source'] == 'sensor1'
    #     assert data_point['meta']['source_id'] == '12345'
    #     assert 'location' not in data_point

    # def test_resolve_data(self, logger):
    #     data_point = {
    #         'time': '2023-10-01T12:00:00',
    #         'location': {'latitude': 40.7128, 'longitude': -74.0060},
    #         'source': 'sensor1',
    #         'source_id': '12345',
    #         'value': 42
    #     }
    #     resolved_data = resolve_data(data_point, logger)
    #     assert resolved_data['time'].tzinfo is not None
    #     assert resolved_data['location']['type'] == 'Point'
    #     assert resolved_data['meta']['source'] == 'sensor1'