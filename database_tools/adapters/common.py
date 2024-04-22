"""Common adapter functionality"""
import abc
import os
from typing import Optional

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, Session

_DB_SCHEMA = os.environ.get('DB_SCHEMA')


class AbstractAdapter(metaclass=abc.ABCMeta):
    """Abstract adapter

    Class implements common API for adapters. It is abstract class so it can not be instantiated.
    """

    def __init__(self, schema_name: str, *args, **kwargs) -> None:
        self._schema = schema_name
        self._engine = create_engine(*args, **kwargs)
        self._session_cls = sessionmaker(bind=self._engine)

    @classmethod
    def from_env_vars(cls, schema_name: Optional[str] = None, *args, **kwargs) -> 'AbstractAdapter':
        """Get adapter class initialized from environment variables"""
        obj = cls(
            schema_name=schema_name or _DB_SCHEMA,
            *args,
            **kwargs
        )
        return obj

    @property
    def engine(self):
        """SQLAlchemy engine"""
        return self._engine

    def get_metadata(self) -> MetaData:
        """Get SQLAlchemy metadata"""
        return MetaData(schema=self._schema)

    def get_session(self, **kwargs) -> Session:
        """Create and return new session instance"""
        return self._session_cls(**kwargs)

    def clean(self) -> None:
        """Close all database connections"""
        self._engine.dispose()

    def connection_manager(self):
        """Context manager for raw database connection management."""
        raise NotImplementedError("Connection management is not implemented for this adapter.")

    def session_manager(self):
        """Context manager for database session management."""
        raise NotImplementedError("Session management is not implemented for this adapter.")

    def execute_sql(self, sql: str):
        """Execute a raw SQL command."""
        raise NotImplementedError("Executing SQL is not implemented for this adapter.")

    def inspect_engine(self):
        """Inspect the current engine."""
        raise NotImplementedError("Engine inspector is not implemented for this adapter.")

    def bulk_insert(self, the_class, dict_list):
        """Bulk insert a list of dictionaries into the corresponding table."""
        raise NotImplementedError("bulk insert is not implemented for this adapter.")
