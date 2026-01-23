# src/databases/benchmark_db/models.py

from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, Boolean, Float, BigInteger, DateTime

from src.databases.benchmark_db.base import Base


def utc_now():
    return datetime.now(timezone.utc)


class Benchmark(Base):
    __tablename__ = "benchmark"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # identifiers
    benchmark_name = Column(String, nullable=False)   # e.g. "ingestion.insert_many.fixed_batch"
    database_name = Column(String, nullable=False)    # e.g. "mongodb"
    dataset_name = Column(String, nullable=False)     # e.g. "smart_ocean_sample_v1"

    # workload size
    total_points = Column(Integer, nullable=False)
    batch_size = Column(Integer, nullable=False)
    payload_bytes = Column(BigInteger, nullable=False)

    # performance results
    total_time_seconds = Column(Float, nullable=False)
    throughput_points_per_sec = Column(Float, nullable=False)

    # status
    success = Column(Boolean, nullable=False, default=True)

    # metadata
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
