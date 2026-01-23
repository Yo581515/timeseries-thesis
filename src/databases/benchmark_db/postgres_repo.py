# src/databases/benchmark_db/postgres_repo.py

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Type, TypeVar

from sqlalchemy import text, select, update as sa_update, delete as sa_delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.databases.benchmark_db.client import PostgresClient
from src.databases.benchmark_db.config import PostgresBConfig

T = TypeVar("T")


class PostgresRepo(PostgresClient):
    """
    Generic repository for SQLAlchemy models.
    Subclasses should set: model = <SQLAlchemy ORM class>
    """
    model: Optional[Type[T]] = None

    def __init__(self, postgres_config: PostgresBConfig, logger: logging.Logger):
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

    def insert_many(self, objs: Iterable[T], *, chunk_size: int = 5000) -> int:
        model = self._require_model()
        objs_list = list(objs)
        total = len(objs_list)

        if total == 0:
            self.logger.info("insert_many(): nothing to insert for %s", model.__name__)
            return 0

        self.logger.info("insert_many(): inserting %d %s rows (chunk_size=%d)",
                         total, model.__name__, chunk_size)

        inserted = 0
        for i in range(0, total, chunk_size):
            chunk = objs_list[i:i + chunk_size]
            with self.session_scope() as db:
                db.add_all(chunk)
            inserted += len(chunk)

        self.logger.info("insert_many(): inserted %d/%d %s rows", inserted, total, model.__name__)
        return inserted

    # ----------------------------
    # READ
    # ----------------------------
    def get_by_id(self, obj_id: Any) -> Optional[T]:
        model = self._require_model()
        self.logger.debug("get_by_id(): %s id=%s", model.__name__, obj_id)

        with self.session_scope() as db:
            # Session.get is fastest by PK
            obj = db.get(model, obj_id)
            return obj

    def get_one_by_filters(self, filters: Dict[str, Any]) -> Optional[T]:
        model = self._require_model()
        self.logger.debug("get_one_by_filters(): %s filters=%s", model.__name__, filters)

        stmt = select(model)
        for k, v in filters.items():
            stmt = stmt.where(getattr(model, k) == v)

        with self.session_scope() as db:
            return db.execute(stmt).scalars().first()

    def list_by_filters(
        self,
        filters: Optional[Dict[str, Any]] = None,
        *,
        limit: int = 1000,
        offset: int = 0,
        order_by: Optional[Any] = None,
        desc: bool = False
    ) -> List[T]:
        model = self._require_model()
        filters = filters or {}

        self.logger.debug(
            "list_by_filters(): %s filters=%s limit=%d offset=%d",
            model.__name__, filters, limit, offset
        )

        stmt = select(model)

        for k, v in filters.items():
            stmt = stmt.where(getattr(model, k) == v)

        if order_by is not None:
            stmt = stmt.order_by(order_by.desc() if desc else order_by.asc())

        stmt = stmt.limit(limit).offset(offset)

        with self.session_scope() as db:
            return list(db.execute(stmt).scalars().all())

    def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        # Simple approach: count via select + scalars length is not ideal for huge tables.
        # If you want true COUNT(*), tell me and I’ll tailor to your SQLAlchemy version.
        model = self._require_model()
        filters = filters or {}

        stmt = select(model)
        for k, v in filters.items():
            stmt = stmt.where(getattr(model, k) == v)

        with self.session_scope() as db:
            n = len(db.execute(stmt).scalars().all())
            self.logger.debug("count(): %s filters=%s -> %d", model.__name__, filters, n)
            return n

    # ----------------------------
    # UPDATE
    # ----------------------------
    def update_by_id(self, obj_id: Any, values: Dict[str, Any]) -> bool:
        model = self._require_model()
        if not values:
            self.logger.info("update_by_id(): no values provided for %s id=%s", model.__name__, obj_id)
            return False

        self.logger.info("update_by_id(): %s id=%s values=%s", model.__name__, obj_id, values)

        stmt = sa_update(model).where(model.id == obj_id).values(**values)
        with self.session_scope() as db:
            res = db.execute(stmt)
            updated = res.rowcount or 0

        self.logger.info("update_by_id(): updated=%d for %s id=%s", updated, model.__name__, obj_id)
        return updated > 0

    def update_by_filters(self, filters: Dict[str, Any], values: Dict[str, Any]) -> int:
        model = self._require_model()
        if not values:
            self.logger.info("update_by_filters(): no values provided for %s", model.__name__)
            return 0

        stmt = sa_update(model)
        for k, v in (filters or {}).items():
            stmt = stmt.where(getattr(model, k) == v)
        stmt = stmt.values(**values)

        self.logger.info("update_by_filters(): %s filters=%s values=%s", model.__name__, filters, values)

        with self.session_scope() as db:
            res = db.execute(stmt)
            updated = res.rowcount or 0

        self.logger.info("update_by_filters(): updated=%d for %s", updated, model.__name__)
        return updated

    # ----------------------------
    # DELETE
    # ----------------------------
    def delete_by_id(self, obj_id: Any) -> bool:
        model = self._require_model()
        self.logger.info("delete_by_id(): %s id=%s", model.__name__, obj_id)

        stmt = sa_delete(model).where(model.id == obj_id)
        with self.session_scope() as db:
            res = db.execute(stmt)
            deleted = res.rowcount or 0

        self.logger.info("delete_by_id(): deleted=%d for %s id=%s", deleted, model.__name__, obj_id)
        return deleted > 0

    def delete_by_filters(self, filters: Dict[str, Any]) -> int:
        model = self._require_model()
        self.logger.info("delete_by_filters(): %s filters=%s", model.__name__, filters)

        stmt = sa_delete(model)
        for k, v in (filters or {}).items():
            stmt = stmt.where(getattr(model, k) == v)

        with self.session_scope() as db:
            res = db.execute(stmt)
            deleted = res.rowcount or 0

        self.logger.info("delete_by_filters(): deleted=%d for %s", deleted, model.__name__)
        return deleted
