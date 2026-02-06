# src/databases/mongodb/setup/insert_example.py

import time
from pprint import pprint

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.databases.mongodb.utils.data_utils import resolve_data, convert_time
from src.validators.json_validator import is_obj_valid_json


if __name__ == "__main__":
    config_file_path = "./configs/config-mgdb.yml"
    config = load_config(config_file_path)
    
    # pprint(config)

    logger = get_logger("insert_example.py", config["general"]["log_file"])

    mongodb_config = get_mongodb_config(config["database"])
    # print(mongodb_config)
    repo = MongoDBRepository(mongodb_config, logger)

    data_file = load_json_data(config["general"]["mongodb_data_file"])
    logger.info("Loaded %d documents", len(data_file))


    # print(is_obj_valid_json(data_file))
    print()

    # optional cleanup/formatting
    data = [doc for doc in data_file if convert_time(doc, logger)]
    
    # pprint(data[:2])


    repo.connect_and_cache()

    try:
        repo.ping()

        # # reset collection for test
        repo.delete_by_query({})

        # insert 3 docs
        start = time.time()
        repo.insert_one(data[0])
        end = time.time()
        print(f"Insert first single doc time: {end - start:.4f} sec")
        
        start = time.time()
        repo.insert_one(data[1])
        end = time.time()
        print(f"Insert second single doc time: {end - start:.4f} sec")
        
        start = time.time()
        repo.insert_one(data[2])
        end = time.time()
        print(f"Insert third single doc time: {end - start:.4f} sec")

        # query example
        start = time.time()
        docs = repo.find_by_query({"meta.source": "Node 1"})
        end = time.time()
        print(f"Query time: {end - start:.4f} sec")
        # pprint(docs[:2])

    finally:
        repo.disconnect()