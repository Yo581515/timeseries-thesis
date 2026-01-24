# src/databases/benchmark_db/bechmark_db.py

import logging

from src.databases.benchmark_db.postgres_repo import PostgresRepo
from src.databases.benchmark_db.config import PostgresBConfig
from src.databases.benchmark_db.models import Benchmark  # IMPORTANT: registers model with Base.metadata


class BenchmarkDB(PostgresRepo):
    model = Benchmark

    def __init__(self, postgres_config: PostgresBConfig, logger: logging.Logger):
        super().__init__(postgres_config, logger)


if __name__ == "__main__":
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.benchmark_db.config import get_postgres_config

    bmdb_config_file_path = "./configs/config-benchmark_db.yml"
    bmdb_config_dict = load_config(bmdb_config_file_path)
    logger = get_logger("benchmark_db", bmdb_config_dict["general"]["log_file"])

    benchmark_db_config = get_postgres_config(bmdb_config_dict["postgres"])
    bm_db = BenchmarkDB(benchmark_db_config, logger)

    if bm_db.connect():
        bm_db.create_tables()  # creates Benchmark table
        bm_db.ping()

        entry = Benchmark(
            benchmark_name="ingestion.insert_many.fixed_batch",
            database_name="postgresql",
            dataset_name="smart_ocean_sample_v1",
            total_points=100000,
            batch_size=1000,
            payload_bytes=2048000,
            total_time_seconds=12.34,
            throughput_points_per_sec=8100.5,
            success=True,
        )
        bm_db.insert(entry)
        # bm_db.clear_table()
        bm_db.disconnect()
