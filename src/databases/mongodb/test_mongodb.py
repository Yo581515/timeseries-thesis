# src/databases/mongodb/test_mongodb.py

import time
from pprint import pprint

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.databases.mongodb.utils.data_utils import resolve_data
from src.validators.json_validator import is_obj_valid_json


if __name__ == "__main__":
    config_file_path = "./configs/config-mgdb-fwd.yml"
    config = load_config(config_file_path)
    
    pprint(config)

    logger = get_logger("test_mongodb.py", config["general"]["log_file"])

    mongodb_config = get_mongodb_config(config["mongodb"])
    print(mongodb_config)
    repo = MongoDBRepository(mongodb_config, logger)

    data_file = load_json_data(config["general"]["data_file"])
    logger.info("Loaded %d documents", len(data_file))


    print(is_obj_valid_json(data_file))
    print()

    # optional cleanup/formatting
    data = [doc for doc in data_file if resolve_data(doc, logger)]
    


    print(repo.connect_and_cache())

    try:
        repo.ping()

        # # reset collection for test
        repo.delete_by_query({})

        # insert 3 docs
        start = time.time()
        repo.insert_one(data[0])
        repo.insert_one(data[1])
        repo.insert_one(data[2])
        end = time.time()
        logger.info("Inserted 3 docs in %.4f sec", end - start)

        # query example
        docs = repo.find_by_query({"meta.source": "Node 1"})
        pprint(docs[:2])

    finally:
        repo.disconnect()