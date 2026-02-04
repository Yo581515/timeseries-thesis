# src/benchmark/benchmark_mongodb_setup.py

from pydoc import doc
import time
from pprint import pprint
from pathlib import Path


from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.mongodb.config import get_mongodb_config

from src.databases.mongodb.utils.data_utils import resolve_data, convert_time, load_mongodb_json
from src.validators.json_validator import is_obj_valid_json

lines = ""
for i in range(50):
    lines += "-"
    

print(lines)
# 1 load data from folder
config_file_path = "./configs/config-mgdb-fwd.yml"
config = load_config(config_file_path)
logger = get_logger("test_mongodb.py", config["general"]["log_file"])
mongodb_config = get_mongodb_config(config["mongodb"])
mongodb_data_folder_path = config["general"]["mongodb_data_folder_path"]
folder = Path(mongodb_data_folder_path)
file_names = [p.name for p in folder.glob("*.json")]
mongodb_data_obs = [load_mongodb_json(str(folder / file_name), logger) for file_name in file_names]
mongodb_format_data = []
for docs in mongodb_data_obs:
    if is_obj_valid_json(docs):
        for doc in docs:
            convert_time(doc, logger)
        mongodb_format_data.append(docs) 
# pprint(mongodb_format_data[0][0])

print(lines)
# 2 setup mongodb connection
from src.databases.mongodb.mongodb_repository import MongoDBRepository
mongodb_repo = MongoDBRepository(mongodb_config, logger)
mongodb_repo.connect_and_cache()
if mongodb_repo.ping():
    print("MongoDB connection successful.")
    mongodb_repo.delete_by_query({})  # clear previous benchmark entries


print(lines)
# 3 setup benchmark database
from src.databases.benchmark_db.models import Benchmark 
from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.benchmark_db.config import get_postgres_config
from src.databases.benchmark_db.benchmark_db import BenchmarkDB

bmdb_config_file_path = "./configs/config-benchmark_db.yml"
bmdb_config = load_config(bmdb_config_file_path)
logger = get_logger("benchmark_db", bmdb_config["general"]["log_file"])

benchmark_db_config = get_postgres_config(bmdb_config["postgres"])
bm_db = BenchmarkDB(benchmark_db_config, logger)

try:
    bm_db.connect()
    bm_db.create_tables()  # creates Benchmark table
    bm_db.ping()
    print("BenchmarkDB is connected and ready.")
    
    bm_db.clear_table()
    print("Benchmark table cleared.")
except Exception as e:
    logger.error(f"An error occurred: {e}")

print(lines)    


# 4 benchmark save
# REPEATS = 100
# FAIL_EVERY = 9  # every 9th iteration fails (i=0,9,18,...)

# # mongodb_repo.connect()
# # bm_db.connect()

# try:
#     docs = mongodb_format_data[1]
#     dataset = file_names[1]
#     n_points = len(docs)

#     for i in range(REPEATS):
#         start = time.perf_counter()
#         success = True
#         err_msg = None

#         try:
#             # Inject failure BEFORE insert
#             if i % FAIL_EVERY == 0:
#                 raise RuntimeError("Injected failure for benchmarking (every 9th run)")

#             repo.insert_many(docs)

#         except Exception as e:
#             success = False
#             err_msg = str(e)

#         end = time.perf_counter()
#         total_seconds = end - start
#         throughput = (n_points / total_seconds) if total_seconds > 0 else 0.0

#         entry = Benchmark(
#             benchmark_name="ingestion.insert_many.fixed_batch",
#             database_name="mongodb",
#             dataset_name=dataset,
#             total_points=n_points,
#             total_seconds=total_seconds,
#             throughput_points_per_sec=throughput,
#             success=success,
#         )

#         # Record the benchmark result (even failed ones)
#         bm_db.insert(entry)

#         print(
#             f"Run {i+1}/{REPEATS} | success={success} | "
#             f"time={total_seconds:.4f}s | thr={throughput:.1f} pts/s"
#             + (f" | err={err_msg}" if err_msg else "")
#         )

# finally:
#     mongodb_repo.disconnect()
#     bm_db.disconnect()


REPEATS = 1000

FAIL_EVERY = 66  # fail on run 67 

# mongodb_repo.connect_and_cache()   # or mongodb_repo.connect() depending on your repo
# bm_db.connect()

try:
    for file_idx in range(len(mongodb_format_data) - 1, -1, -1):
        docs = mongodb_format_data[file_idx]
        dataset = file_names[file_idx]
        n_points = len(docs)

        print(f"\n=== Dataset {file_idx+1}/{len(mongodb_format_data)}: {dataset} ({n_points} points) ===")

        

        for run_idx in range(REPEATS):
            
            start = time.perf_counter()
            success = True
            err_msg = None

            try:
                # Inject failure BEFORE insert
                if run_idx % FAIL_EVERY == 0 and run_idx != 0:
                    raise RuntimeError("Injected failure for benchmarking")

                mongodb_repo.insert_many(docs)

            except Exception as e:
                success = False
                err_msg = str(e)

            end = time.perf_counter()
            total_seconds = end - start

            # Throughput should be meaningful only on success
            throughput = (n_points / total_seconds) if (success and total_seconds > 0) else 0.0

            entry = Benchmark(
                benchmark_name="ingestion.insert_many.fixed_batch",
                database_name="mongodb",
                dataset_name=dataset,
                total_points=n_points,
                total_seconds=total_seconds,
                throughput_points_per_sec=throughput,
                success=success,
            )

            bm_db.insert(entry)
            mongodb_repo.delete_by_query({})

            print(
                f"Run {run_idx+1}/{REPEATS} | success={success} | "
                f"time={total_seconds:.4f}s | thr={throughput:.1f} pts/s"
                + (f" | err={err_msg}" if err_msg else "")
            )

finally:
    mongodb_repo.disconnect()
    bm_db.disconnect()


# # get all benchmark entries
# try:
#     all_entries = bm_db.get_all()
#     print(f"Total benchmark entries: {len(all_entries)}")
#     if len(all_entries) > 0:
#         first_entry = all_entries[0]
#         print("First benchmark entry:")
#         print(first_entry)
# except Exception as e:
#     logger.error(f"An error occurred while retrieving benchmark entries: {e}")
# finally:
#     bm_db.disconnect()
# print(lines)    