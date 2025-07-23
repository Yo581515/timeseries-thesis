# src/databases/mongodb/test_mongodb.py

from pprint import pprint
import random
import time

from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.configs.mongodb_config import get_mongodb_config
from src.databases.mongodb.mongodb import MongoDBClient

from src.databases.mongodb.mongodb_utils.utils import resolve_data


def rand_float(low, high, precision):
    return round(random.uniform(low, high), precision)
  
if __name__ == '__main__':

    config = load_config(f'configs/config-mgdb-fwd.yml')
    
    logger = get_logger('test_mongodb', config['general']['log_file'])

    
    data = load_json_data(config['general']['data_file'])
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
        
        print("using find_by_query:")
        pprint(mongodb_repo.find_by_query({"meta.source": "Node 1"}))
        print()
        print("using aggregate:")
        pipeline = [
            {"$match": {"meta.source": "Node 1"}},
            {"$project": {"time": 1, "_id": 1}}
        ]
        pprint(mongodb_repo.aggregate(pipeline))