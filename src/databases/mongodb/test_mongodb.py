# src/databases/mongodb/test_mongodb.py

import random
from pprint import pprint

from src.databases.mongodb.mongodb_repo import MongoDBRepository
from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.configs.mongodb_config import get_mongodb_config
from src.databases.mongodb.mongodb import MongoDBClient
from src.databases.mongodb.mongodb_utils import resolve_data


def rand_float(low, high, precision):
    return round(random.uniform(low, high), precision)
  
if __name__ == '__main__':

    config = load_config(f'configs/config-mgdb-fwd.yml')
    
    logger = get_logger('test_mongodb', config['general']['log_file'])

    
    data = load_json_data(config['general']['data_file'])
    for doc in data:
        doc = resolve_data(doc, logger)
    # pprint(data[0])

    mongodb_config = get_mongodb_config(config_dict=config['mongodb'])
    mongoDBClient = MongoDBClient(mongodb_config=mongodb_config, logger=logger)

    mongodb_repo = MongoDBRepository(mongoDBClient)
    
    mongodb_repo.drop_collection()
    
    