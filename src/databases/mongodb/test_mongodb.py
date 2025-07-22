# src/databases/mongodb/test_mongodb.py

from datetime import datetime, UTC
import random
from pprint import pprint

import time
from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.configs.mongodb_config import get_mongodb_config
from src.databases.mongodb.mongodb import MongoDBClient


def rand_float(low, high, precision):
    return round(random.uniform(low, high), precision)
  
if __name__ == '__main__':
    
    config = load_config('configs\config-mgdb-fwd.yml')

    logging = get_logger('test_mongodb', config['general']['log_file'])

    data = load_json_data(config['general']['data_file'])
    
    data =  {
        "time": datetime.now(UTC),  # equivalent to $$NOW, $$NOW is only for aggregation
        "sensor_id": "sensor_001",
        "temperature": rand_float(20, 30, 1),
        "humidity": rand_float(30, 60, 1),
        "pressure": rand_float(1000, 1020, 1),
        "location": {
            "type": "Point",
            "coordinates": [
                rand_float(-180, 180, 6),  # longitude
                rand_float(-90, 90, 6)     # latitude
            ]
        }
    }
    
    mongodb_config = get_mongodb_config(config['mongodb'])
    client = MongoDBClient(mongodb_config=mongodb_config, logging=logging)

    # client.ping()

    for i in range(10):
        data =  {
            "time": datetime.now(UTC),  # equivalent to $$NOW, $$NOW is only for aggregation
            "sensor_id": f'sensor_{i:03}',
            "temperature": rand_float(20, 30, 1),
            "humidity": rand_float(30, 60, 1),
            "pressure": rand_float(1000, 1020, 1),
            "location": {
                "type": "Point",
                "coordinates": [
                    rand_float(-180, 180, 6),  # longitude
                    rand_float(-90, 90, 6)     # latitude
                ]
            }
        }
        client.insert_one(data)
        time.sleep(1)
        