"""Async PostgreSQL adapter built on existing AbstractAdapter"""
import ssl
from contextlib import asynccontextmanager
from urllib.parse import quote_plus as urlquote
from typing import Optional, List, Dict, Any

from sqlalchemy import MetaData, text, inspect, insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession, async_sessionmaker

from database_tools.adapters.common import AbstractAdapter  # your existing base


class AsyncPostgresqlAdapter(AbstractAdapter):
    """Async PostgreSQL adapter using SQLAlchemy’s async engine (asyncpg)."""

    CONNECTION_STRING_TEMPLATE = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"

    def __init__(
        self,
        database_name: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
        schema_name: str = "public",
        ssl_required: bool = False,
        application_name: Optional[str] = None,
        echo: bool = False,
        **kwargs,
    ):
        if user:
            user = urlquote(user)
        if password:
            password = urlquote(password)

        dsn = self.CONNECTION_STRING_TEMPLATE.format(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database_name,
        )

        connect_args = {}
        if ssl_required:
            connect_args["ssl"] = ssl.create_default_context()
        if application_name:
            connect_args.setdefault("server_settings", {})["application_name"] = str(application_name)

        # Initialize async engine & session factory
        self._schema = schema_name
        self._engine: AsyncEngine = create_async_engine(dsn, echo=echo, connect_args=connect_args, **kwargs)
        self._session_cls = async_sessionmaker(bind=self._engine, expire_on_commit=False)

    # ---------- overrides ----------

    @asynccontextmanager
    async def connection_manager(self):
        async with self._engine.begin() as conn:
            yield conn

    @asynccontextmanager
    async def session_manager(self):
        session: AsyncSession = self._session_cls()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def init_schema(self, metadata: MetaData) -> None:
        schemas = {t.schema for t in metadata.tables.values() if t.schema}
        async with self._engine.begin() as conn:
            for schema in schemas:
                await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            await conn.run_sync(metadata.create_all)

    async def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None):
        async with self.session_manager() as session:
            await session.execute(text(sql), params or {})

    async def inspect_engine(self):
        async with self._engine.connect() as conn:
            return await conn.run_sync(lambda sync_conn: inspect(sync_conn))

    async def bulk_insert(self, orm_class, dict_list: List[Dict[str, Any]], ignore_conflicts=False):
        if not dict_list:
            return
        stmt = insert(orm_class.__table__)
        if ignore_conflicts:
            stmt = stmt.on_conflict_do_nothing()
        async with self.session_manager() as session:
            await session.execute(stmt, dict_list)

    async def clean(self):
        """Dispose of the engine."""
        await self._engine.dispose()
