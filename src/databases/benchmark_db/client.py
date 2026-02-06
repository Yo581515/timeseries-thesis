# src/databases/benchmark_db/client.py

import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.databases.benchmark_db.base import Base
from src.databases.benchmark_db.config import BenchmarkDBConfig


class BenchmarkDBClient:
    def __init__(self, postgres_config: BenchmarkDBConfig, logger: logging.Logger):
        self.logger = logger

        self.user = postgres_config.BENCHMARKDB_USER
        self.password = postgres_config.BENCHMARKDB_PASSWORD
        self.host = postgres_config.BENCHMARKDB_HOST
        self.port = postgres_config.BENCHMARKDB_PORT
        self.database = postgres_config.BENCHMARKDB_DATABASE
        self.schema = postgres_config.BENCHMARKDB_SCHEMA

        self.url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

        self.engine = None
        self.SessionLocal = None

    def connect(self) -> bool:
        try:
            self.logger.info("Connecting to Postgres...")

            # Set schema as default via search_path
            self.engine = create_engine(
                self.url,
                connect_args={"options": f"-csearch_path={self.schema}"},
                pool_pre_ping=True,
            )
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

            # Ensure schema exists
            with self.engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"'))

            # Smoke test connection
            db = self.SessionLocal()
            try:
                db.execute(text("SELECT 1"))
            finally:
                db.close()

            self.logger.info("Connected to Postgres successfully.")
            return True
        except Exception as e:
            self.logger.error("Connection error: %s", e)
            return False

    def create_tables(self) -> bool:
        """
        Creates tables for all models imported into Base.metadata.
        Make sure you import src.databases.benchmark_db.models before calling this.
        """
        try:
            if not self.engine:
                raise RuntimeError("Engine not initialized. Call connect() first.")

            Base.metadata.create_all(bind=self.engine)
            self.logger.info("Tables created successfully.")
            return True
        except Exception as e:
            self.logger.error("Create tables error: %s", e)
            return False

    def disconnect(self) -> bool:
        try:
            self.logger.info("Disconnecting from Postgres...")
            if self.engine:
                self.engine.dispose()
            self.logger.info("Disconnected from Postgres.")
            return True
        except Exception as e:
            self.logger.error("Disconnection error: %s", e)
            return False


if __name__ == "__main__":
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.benchmark_db.config import get_postgres_config

    bmdb_config_file_path = "./configs/config-benchmark.yml"
    bmdb_config_dict = load_config(bmdb_config_file_path)
    logger = get_logger("client.py", bmdb_config_dict["general"]["log_file"])

    benchmark_db_config = get_postgres_config(bmdb_config_dict["database"])
    
    print(benchmark_db_config)
    
    bmc = BenchmarkDBClient(benchmark_db_config, logger)
    
    print(bmc.connect() )
    
    bmc.create_tables()
    bmc.disconnect()