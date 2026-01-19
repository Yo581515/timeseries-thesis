# src/databases/benchmark_db/models.py

import pprint
from database import Base, SessionLocal, engine, logger
from sqlalchemy import Column, Integer, String, Boolean, Float, BigInteger, DateTime
from datetime import datetime


class BenchmarkRun(Base):
    __tablename__ = "benchmark_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # identifiers
    benchmark_name = Column(String, nullable=False)   # e.g. "ingestion.insert_many.fixed_batch"
    database_name = Column(String, nullable=False)    # e.g. "mongodb"
    dataset_name = Column(String, nullable=False)     # e.g. "smart_ocean_sample_v1"

    # workload size
    total_points = Column(Integer, nullable=False)    # number of datapoints
    batch_size = Column(Integer, nullable=False)      # insert batch size
    payload_bytes = Column(BigInteger, nullable=False)  # total data size in bytes

    # performance results
    total_time_seconds = Column(Float, nullable=False)
    throughput_points_per_sec = Column(Float, nullable=False)

    # status
    success = Column(Boolean, nullable=False, default=True)

    # metadata
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)



if __name__ == "__main__":
    # Create tables
    Base.metadata.create_all(bind=engine)
    logger.info("BenchmarkRun table created successfully.")

    example_run = BenchmarkRun(
        benchmark_name="ingestion.insert_many.fixed_batch",
        database_name="mongodb",
        dataset_name="smart_ocean_sample_v1",
        total_points=100000,
        batch_size=1000,
        payload_bytes=2048000,
        total_time_seconds=45.67,
        throughput_points_per_sec=2189.5,
        success=True
    )

    logger.info(f"Example BenchmarkRun instance: {example_run}")
    
    db = SessionLocal()

    try:
        # ✅ Clear table (delete all rows)
        db.query(BenchmarkRun).delete()
        db.commit()
        logger.info("benchmark_runs table cleared successfully.")

        # Insert example row
        db.add(example_run)
        db.commit()
        logger.info("Example BenchmarkRun instance added to the database.")

    except Exception as e:
        db.rollback()
        logger.error(f"DB error: {e}")

    finally:
        db.close()