import logging
from pprint import pprint

from src.databases.benchmark_db.postgres_repo import BenchmarkDBRepo
from src.databases.benchmark_db.config import BenchmarkDBConfig
from src.databases.benchmark_db.models import BenchmarkOne  # IMPORTANT: registers model with Base.metadata


class BenchmarkDB(BenchmarkDBRepo):
    model = BenchmarkOne

    def __init__(self, benchmark_db_config: BenchmarkDBConfig, logger: logging.Logger):
        super().__init__(benchmark_db_config, logger)


if __name__ == "__main__":
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.benchmark_db.config import get_postgres_config

    bmdb_config_file_path = "./configs/config-benchmarkdb.yml"
    bmdb_config_dict = load_config(bmdb_config_file_path)
    logger = get_logger("benchmark_db.py", bmdb_config_dict["general"]["log_file"])

    benchmark_db_config = get_postgres_config(bmdb_config_dict["database"])
    bm_db = BenchmarkDB(benchmark_db_config, logger)

    try:
        bm_db.connect()
        bm_db.create_tables()  # creates Benchmark table
        bm_db.ping()
        print("BenchmarkDB is connected and ready.")

        entry = BenchmarkOne(
            benchmark_name="ingestion.insert_many.fixed_batch",
            database_name="mongodb",
            dataset_name="smart_ocean_sample_v1",
            total_points=100000,
            total_seconds=12.34,
            throughput_points_per_sec=8100.0,
            success=True
        )
        bm_db.insert(entry)
        print("Inserted benchmark entry:")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        bm_db.disconnect()


    print("\nNow testing querying benchmark entries...")
# quiry
    try:
        bm_db.connect()
        bm_db.create_tables()  # creates Benchmark table
        bm_db.ping()
        data_points = bm_db.get_all()
        print(len(data_points))
        print("All entries:")
        
        if len(data_points) > 0:
            print("First entry:")
            d_p = data_points[0]
            print(d_p) 
            # print()
                
        # bm_db.clear_table()
    except Exception as e:
        logger.error(f"An error occurred during querying: {e}")
        