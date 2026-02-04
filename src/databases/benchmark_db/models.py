# src/databases/benchmark_db/models.py

from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, Boolean, Float, DateTime
from src.databases.benchmark_db.base import Base


def utc_now() -> datetime:
    """UTC timestamp for created_at."""
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

    # performance results
    total_seconds = Column(Float, nullable=False)
    throughput_points_per_sec = Column(Float, nullable=False)

    # status
    success = Column(Boolean, nullable=False, default=True)

    # metadata
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)


    def __str__(self) -> str:
        # Human-friendly, multi-line summary (safe and readable in terminal/logs).
        created_at = self.created_at.isoformat() if self.created_at else "N/A"
        return (
            f"Benchmark {self.id}\n"
            f"  name:        {self.benchmark_name}\n"
            f"  database:    {self.database_name}\n"
            f"  dataset:     {self.dataset_name}\n"
            f"  points:      {self.total_points}\n"
            f"  total time:  {self.total_seconds:.4f} s\n"
            f"  throughput:  {self.throughput_points_per_sec:.2f} pts/s\n"
            f"  success:     {self.success}\n"
            f"  created at:  {created_at}"
        )
        
        
    def to_dict(self) -> dict:
        # Convert to dict for easy serialization (e.g. JSON).
        return {
            "id": self.id,
            "benchmark_name": self.benchmark_name,
            "database_name": self.database_name,
            "dataset_name": self.dataset_name,
            "total_points": self.total_points,
            "total_seconds": self.total_seconds,
            "throughput_points_per_sec": self.throughput_points_per_sec,
            "success": self.success,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }