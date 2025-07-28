# src/databases/mongodb/test_mongodb.py

from pprint import pprint
import random
import time
import os

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.mongodb_repository import MongoDBRepository

from src.databases.mongodb.utils.data_utils import resolve_data


def rand_float(low, high, precision):
    return round(random.uniform(low, high), precision)
  
if __name__ == '__main__':

    config = {
        'general': {
            'mongodb_data_file': 'data/sample_data.json',
            'mongodb_log_file': 'logs/mongodb_forwarder.log'
        },
        'mongodb': {
            'MONGO_DB_USER': os.getenv('MONGO_DB_USER', ''),
            'MONGO_DB_PASSWORD': os.getenv('MONGO_DB_PASSWORD', ''),
            'MONGO_DB_CLUSTER': os.getenv('MONGO_DB_CLUSTER', ''),
            'MONGODB_DATABASE_NAME': os.getenv('MONGODB_TIMESERIES_DATABASE_NAME', ''),
            'MONGODB_COLLECTION_NAME': os.getenv('MONGODB_TIMESERIES_COLLECTION_NAME', '')
        }
    }
    logger = get_logger('test_mongodb', config['general']['mongodb_log_file'])

    
    data = load_json_data(config['general']['mongodb_data_file'])
    for i, doc in enumerate(data):
        data[i] = resolve_data(doc, logger)
    # pprint(data[0])
    # print()

    mongodb_config = get_mongodb_config(config_dict=config['mongodb'])
    mongoDBClient = MongoDBClient(mongodb_config=mongodb_config, logger=logger)
    mongodb_repo = MongoDBRepository(mongoDBClient)
    
    if mongodb_repo.ping():
        mongodb_repo.delete_by_query({})
        start = time.time()
        mongodb_repo.insert_one(data[0])
        mongodb_repo.insert_one(data[1])
        mongodb_repo.insert_one(data[2])
        end = time.time()
        pprint(f"Inserted in {end - start:.2f} seconds.")
        
        start = time.time()
        print("using find_by_query:")
        pprint(mongodb_repo.find_by_query({"meta.source": "Node 1"}))
        end = time.time()
        print(f"Find by query took {end - start:.2f} seconds.")

        print("using aggregate:")
        pipeline = [
            {"$match": {"meta.source": "Node 1"}},
            {"$project": {"time": 1, "_id": 1}}
        ]
        start = time.time()
        pprint(mongodb_repo.aggregate(pipeline))
        end = time.time()
        print(f"Aggregated in {end - start:.2f} seconds.")