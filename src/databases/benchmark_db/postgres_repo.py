# src/databases/benchmark_db/postgres_repo.py

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Type, TypeVar

from sqlalchemy import text, select, update as sa_update, delete as sa_delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.databases.benchmark_db.client import BenchmarkDBClient
from src.databases.benchmark_db.config import BenchmarkDBConfig

T = TypeVar("T")


class BenchmarkDBRepo(BenchmarkDBClient):
    """
    Generic repository for SQLAlchemy models.
    Subclasses should set: model = <SQLAlchemy ORM class>
    """
    model: Optional[Type[T]] = None

    def __init__(self, postgres_config: BenchmarkDBConfig, logger: logging.Logger):
        super().__init__(postgres_config, logger)

    # ----------------------------
    # Session handling (reliable)
    # ----------------------------
    @contextmanager
    def session_scope(self) -> Session:
        if not self.SessionLocal:
            raise RuntimeError("SessionLocal is not initialized. Call connect() first.")

        db: Session = self.SessionLocal()
        try:
            yield db
            db.commit()
        except Exception as e:
            db.rollback()
            self.logger.exception("DB transaction failed; rolled back. Error=%s", e)
            raise
        finally:
            db.close()

    def _require_model(self) -> Type[T]:
        if self.model is None:
            raise ValueError("Repo.model is not set. Subclass must define model = <SQLAlchemy model>")
        return self.model

    # ----------------------------
    # Health
    # ----------------------------
    def ping(self) -> bool:
        try:
            with self.session_scope() as db:
                self.logger.info("Pinging Postgres database...")
                db.execute(text("SELECT 1"))
            self.logger.info("Ping OK.")
            return True
        except Exception as e:
            self.logger.error("Ping error: %s", e)
            return False

    # ----------------------------
    # INSERT
    # ----------------------------
    def insert(self, obj: T) -> T:
        model = self._require_model()
        if not isinstance(obj, model):
            self.logger.warning("insert(): object type %s does not match repo model %s",
                                type(obj).__name__, model.__name__)

        self.logger.debug("insert(): inserting one %s", model.__name__)
        with self.session_scope() as db:
            db.add(obj)
            db.flush()      # assign PKs
            db.refresh(obj) # refresh loaded fields
        self.logger.info("insert(): inserted %s", model.__name__)
        return obj
    
    def clear_table(self) -> int:
        model = self._require_model()
        self.logger.debug("clear_table(): deleting all records from %s", model.__name__)
        with self.session_scope() as db:
            deleted = db.query(model).delete()
        self.logger.info("clear_table(): deleted %d rows from %s", deleted, model.__name__)
        return deleted
    
    
    def get_all(self) -> List[T]:
        with self.session_scope() as db:
            try:
                return db.query(self.model).all()
            finally:
                db.close()
                
                
    
    def count(self) -> int:
        model = self._require_model()
        self.logger.debug("count(): counting records in %s", model.__name__)
        with self.session_scope() as db:
            count = db.query(model).count()
        self.logger.info("count(): found %d records in %s", count, model.__name__)
        return count
    
    