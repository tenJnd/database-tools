"""PostgreSQL adapter"""
from typing import Optional, Type
import os
from urllib.parse import quote_plus as urlquote

from sqlalchemy import MetaData
from sqlalchemy.schema import CreateSchema

from cd_data.database.adapters.common import AbstractAdapter


_DB_HOST = os.environ.get('DB_HOST', 'localhost')
_DB_PORT = os.environ.get('DB_PORT', 5432)
_DB_USER = os.environ.get('DB_USER')
_DB_PASSWORD = os.environ.get('DB_PASS')
_DB_NAME = os.environ.get('DB_NAME')
_DB_SSL_REQUIRED = os.environ.get('DB_SSL_REQUIRED', False)


class PostgresqlAdapter(AbstractAdapter):
    """PostgreSQL adapter

    Adapter handles connection and related things to PostgreSQL database.
    """
    # pylint: disable=too-many-arguments

    CONNECTION_STRING_TEMPLATE = 'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

    def __init__(self, database_name: str, user: str, password: str, host: str = 'localhost',
                 port: int = 5432, schema_name: str = 'public', ssl_required: bool = False,
                 application_name: str = None, *args, **kwargs) -> None:
        """PostgreSQL database initialization

        :param host: hostname of database server
        :param port: listening port at host
        :param database_name: database name
        :param user: username for authentication
        :param password: password for authentication
        :param schema_name: database schema name
        :param ssl_required: True if ssl mode is required
        :param application_name: Optional parameter that carries the name of the application.
        """

        if user is not None:
            user = urlquote(user)
        if password is not None:
            password = urlquote(password)

        connection_string = self.CONNECTION_STRING_TEMPLATE.format(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database_name
        )

        connect_args = {}
        if ssl_required:
            connect_args = {'sslmode': 'require'}

        if application_name:
            connect_args.update({"application_name": str(application_name)})

        super().__init__(schema_name, connection_string, connect_args=connect_args, *args, **kwargs)

    @classmethod
    def from_env_vars(cls, user: Optional[str] = None,  # pylint: disable=arguments-differ
                      password: Optional[str] = None, host: Optional[str] = None,
                      port: Optional[int] = None, database_name: Optional[str] = None,
                      ssl_required: Optional[bool] = False, *args, **kwargs
                      ) -> Type['AbstractAdapter']:
        """Create class instance from environment variables"""
        obj = super().from_env_vars(
            host=host or _DB_HOST,
            port=port or _DB_PORT,
            user=user or _DB_USER,
            password=password or _DB_PASSWORD,
            database_name=database_name or _DB_NAME,
            ssl_required=ssl_required or _DB_SSL_REQUIRED,
            *args,
            **kwargs
        )
        return obj

    def init_schema(self, metadata: MetaData) -> None:
        """Initialize database schema according to given metadata"""

        # Create database schema
        if not self._engine.dialect.has_schema(self._engine, self._schema):
            self._engine.execute(CreateSchema(self._schema))

        # Create all defined objects
        metadata.create_all(self._engine)
