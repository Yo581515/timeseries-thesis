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
    
    def __repr__(self):
        return (f"<Benchmark(id={self.id}, benchmark_name='{self.benchmark_name}', "
                f"database_name='{self.database_name}', dataset_name='{self.dataset_name}', "
                f"total_points={self.total_points}, batch_size={self.batch_size}, "
                f"payload_bytes={self.payload_bytes}, total_time_seconds={self.total_time_seconds}, "
                f"throughput_points_per_sec={self.throughput_points_per_sec}, success={self.success}, "
                f"created_at={self.created_at})>")
        

    def __str__(self):
        return (
            f"Id {self.id}\n"
            f"Name: {self.benchmark_name}\n"
            f"Database: {self.database_name}\n"
            f"Dataset: {self.dataset_name}\n"
            f"Total points: {self.total_points}\n"
            f"Batch size: {self.batch_size}\n"
            f"Payload: {self.payload_bytes} bytes\n"
            f"Total time: {self.total_time_seconds:.2f} s\n"
            f"Throughput: {self.throughput_points_per_sec:.2f} pts/s\n"
            f"Success: {self.success}\n"
            f"Created at: {self.created_at.isoformat()}"
        )
