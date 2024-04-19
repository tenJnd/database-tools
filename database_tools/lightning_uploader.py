"""
This module is extension of utils.pd.to_
and was inspired by this great article
https://hakibenita.com/fast-load-data-python-postgresql
It aims to load data into a database lightning-fast and very cheap (memory)
With this module, it's also possible to flexibly alter tables in db
and still be able to load unchanged dataframe

db = PostgresqlAdapter.from_env_vars()
uploader = LightningUploader(db)
uploader.upload_data(*args)
"""
# pylint: disable=missing-function-docstring
# pylint: disable=invalid-name
# pylint: disable=inconsistent-return-statements
import io
import logging
import os
import traceback
from datetime import datetime
from typing import Optional, Any, Iterator, Dict

import pandas as pd
from sqlalchemy import inspect

from database_tools.adapters.postgresql import PostgresqlAdapter

LEVEL = os.environ.get('UPLOADER_LOG_LEVEL', 'INFO').upper()

_logger = logging.getLogger(__name__)
_logger.setLevel(level=LEVEL)

TYPES_MAP = {
    'BIGINT': 'Int64',
    'INTEGER': 'Int64'
}

SEP = '|'


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    if not isinstance(value, list) and pd.isna(value):
        return r'\N'
    return (str(value)
            .replace(SEP, r'/')
            .replace('\\', r'\\')
            .replace('\r', r'\\r')
            .replace('\n', r'\\n')
            .replace('\n', '\\n')
            )


def split(data: pd.DataFrame, n: int = 5_000) -> pd.DataFrame:
    data_copy = data.copy()
    d_1 = data_copy[:n]
    d_2 = data_copy[n:]
    return d_1, d_2


def add_and_stmt(cols: list):
    c = 0
    sql = ''
    for stmt in cols:
        if c > 0:
            sql += ' and '
        sql += f"""{" = ".join(stmt)}"""
        c += 1
    return sql


def add_is_null_stmt(cols: list):
    c = 0
    sql = ''
    for stmt in cols:
        if c > 0:
            sql += ' and '
        sql += f"{stmt} is null"
        c += 1
    return sql


def assert_type_df(data: pd.DataFrame):
    if isinstance(data, pd.DataFrame):
        return data
    assert isinstance(data, list), (f"wrong data type -> "
                                    f"input is {type(data)}."
                                    f" Use pd.DataFrame "
                                    f"or list of dicts, orient='records'")
    assert isinstance(data[0], dict), (f"Wrong data type -> "
                                       f"input is list of {type(data[0])}."
                                       f" Use pd.DataFrame or "
                                       f"list of dicts, orient='records'")
    return pd.DataFrame.from_dict(data)


def db_columns_list_with_table_name(table: str, columns: list):
    return [f'{table}.{x}' for x in columns]


class StringIteratorIO(io.TextIOBase):
    """ String iterator """

    def __init__(self, _iter: Iterator[str]):
        self._iter = _iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


class LightningUploader:
    """
    Lightning-fast uploader
    """

    def __init__(self,
                 schema: str,
                 table: str,
                 database: PostgresqlAdapter.__module__,
                 sample: Optional[int] = 5_000
                 ):

        self._schema = schema
        self._table = table
        self._database = database
        self.sample = sample
        self.columns = ...

    @property
    def schema(self) -> str:
        if self._schema is None:
            return 'public'
        return self._schema

    @schema.setter
    def schema(self, name) -> None:
        _logger.info(f"Setting schema to {name}")
        self._schema = name

    @property
    def table(self) -> str:
        return self._table

    @table.setter
    def table(self, name) -> None:
        _logger.info(f"Setting table to {name}")
        self._table = name

    @property
    def database(self) -> PostgresqlAdapter.__module__:
        if self._database is None:
            return PostgresqlAdapter.from_env_vars()
        return self._database

    @database.setter
    def database(self, db) -> None:
        self._database = db

    @property
    def inspector(self) -> inspect.__module__:
        return inspect(self.database.engine)

    @property
    def upsert_tmp_table(self) -> str:
        return f"{self.table}_upsert_tmp"

    def table_exists(self, table: str) -> bool:
        """ returns bool if table exists in schema """
        return table in self.inspector.get_table_names(schema=self.schema)

    def get_columns(self) -> [list, Dict[str, Any]]:
        """ get columns and their remapped types """
        cols = self.inspector.get_columns(
            table_name=self.table,
            schema=self.schema
        )

        autoincrement_key = [
            col['name'] for col in cols if col.get('autoincrement', False)
        ]

        col_types_all = {
            col['name']: TYPES_MAP.get(str(col['type']))
            for col in cols
            if col['name'] not in autoincrement_key  # do not include autoincrement columns
        }
        col_list = list(col_types_all.keys())

        col_types_clean = {
            k: v for k, v
            in col_types_all.items()
            if v is not None
        }

        return col_list, col_types_clean

    def rectify_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Add or drop columns to/from input DataFrame.
        Database table's columns are the source of truth here.
         - col not in db -> drop from input df
         - col in db but not in input -> create columns in df with NaN
        @param dataframe:
        @return: pd.DataFrame[columns from db]
        """
        db_columns_l, db_columns_and_types = self.get_columns()
        df_columns_l = list(dataframe.columns)

        columns_not_in_df = [x for x in db_columns_l if x not in df_columns_l]
        columns_not_in_db = [x for x in df_columns_l if x not in db_columns_l]

        if columns_not_in_db:
            _logger.warning(f"You are trying to upload columns "
                            f"that are no in the database \n"
                            f"table: {self.schema}.{self.table}\n"
                            f"columns: {columns_not_in_db}, "
                            f"add them into the database!"
                            )

        dataframe[columns_not_in_df] = None
        try:
            dataframe = dataframe.astype(db_columns_and_types)
        except TypeError:
            _logger.error(
                f"There was a problem when converting data types: "
                f"\n{traceback.format_exc()}"
            )

        self.columns = db_columns_l
        return dataframe[db_columns_l]

    def create_tmp_table(self, conflict_columns: list) -> None:
        _logger.info(f"Creating empty tmp table: {self.upsert_tmp_table}")
        sql_drop = f"drop table if exists {self.schema}.{self.upsert_tmp_table}"

        sql_create = f"""
                create table if not exists
                {self.schema}.{self.upsert_tmp_table} as (
                    select * from {self.schema}.{self.table}
                    where 1=2
                );
                """

        sql_index_list = [f"""
            create index if not exists idx_{self.upsert_tmp_table}_{x}
             on {self.schema}.{self.upsert_tmp_table} ({x})
        """ for x in conflict_columns]

        with self.database.engine.begin() as con:
            con.execute(sql_drop)
            con.execute(sql_create)
            for sql_index in sql_index_list:
                _logger.info(f"{sql_index}")
                con.execute(sql_index)

        _logger.debug("Tmp table created")

    def build_u_i_queries(self, conflict_columns: list) -> [str, str]:
        c_t_wt_table = ", ".join(self.columns)

        c_s_w_table = db_columns_list_with_table_name(self.upsert_tmp_table, self.columns)
        c_s_w_table = ", ".join(c_s_w_table)

        conflict_cols_t = [f'{self.table}.{x}' for x in conflict_columns]
        conflict_cols_s = [f'{self.upsert_tmp_table}.{x}' for x in conflict_columns]
        cols = list(zip(conflict_cols_t, conflict_cols_s))

        sql_update = f"""
            update {self.schema}.{self.table}
            set ({c_t_wt_table})
                =
                ({c_s_w_table})
            from {self.schema}.{self.upsert_tmp_table}
            where  1=1
            and ({add_and_stmt(cols)})
        """
        _logger.debug(f"Update query \n {sql_update}")

        sql_insert = f"""
            insert into {self.schema}.{self.table}
            select {c_s_w_table}
            from {self.schema}.{self.upsert_tmp_table}
            left join {self.schema}.{self.table}
            on ({add_and_stmt(cols)})
            where ({add_is_null_stmt(conflict_cols_t)})
        """
        _logger.debug(f"Insert query \n {sql_insert}")
        return sql_update, sql_insert

    def check_record_exists(self, sql_condition: str, conn):
        """
        check records existence
        @param sql_condition: string condition to check
        @param conn: connection to db
        @return: record or None
        """
        _logger.info(f"Checking existence of records: {sql_condition}")

        sql_stmt = f"""
        select *
        from {self.schema}.{self.table}
        {sql_condition}
        limit 1
        """

        return conn.execute(sql_stmt).fetchone()

    def delete(self, sql_condition: str) -> None:
        """
        perform delete from table. Sometimes, it's better to first
        delete specific rows from table and then upload. This is faster than upsert.
        example of sql_condition:
        'where cycle_of_origin in (20230628)'
        'where year in (2023) and week in (25)'
        @param sql_condition:
        @return: None
        """
        delete_stmt = f"""
        delete
        from {self.schema}.{self.table}
        {sql_condition}
        """

        with self.database.engine.connect() as conn:
            if self.table_exists(self.table) and self.check_record_exists(sql_condition, conn):
                _logger.info("records exists -> "
                             f"performing DELETE statement:\n{delete_stmt}")
                conn.execute(delete_stmt)
                _logger.info("records DELETED!")
            else:
                _logger.info("No records found within "
                             "this condition -> skipping delete")

    def upload_with_pd(self,
                       sample: pd.DataFrame,
                       replace_if_exists: Optional[bool] = False) -> None:
        """
        upload sample to db using pd.to_sql ->
        pd.to_sql which creates table and loads the sample
        @param sample: int
        @param replace_if_exists: bool
        @return: None
        """
        _logger.debug(f"uploading sample "
                      f"using upload_df..., rows: {len(sample)}")
        if_exists = 'replace' if replace_if_exists else 'append'

        with self.database.engine.connect() as conn:
            sample.to_sql(name=self.table,
                          schema=self.schema,
                          con=conn,
                          if_exists=if_exists)

    def upload_with_copy(self, data: pd.DataFrame, table_name: str) -> None:
        """
        upload data using COPY FROM, this method is fast and cheap
        @param data:
        @param table_name:
        @return: None
        """
        _logger.info(f"Uploading data to {table_name} "
                     f"using copy, rows: {len(data)} ...")

        data = self.rectify_dataframe(data)
        data = data.to_dict(orient='records')

        with self.database.engine.connect() as conn:
            cursor = conn.connection.cursor()
            # bypass for module versions (psycopg2)
            # with bug when assigning schema.table format
            cursor.execute(f'SET search_path TO {self.schema}')

            data_string_iterator = StringIteratorIO((
                SEP.join(map(clean_csv_value, (
                    item[k] for k in item.keys()
                ))) + '\n'
                for item in data
            ))
            cursor.copy_from(
                data_string_iterator, table_name, sep=SEP, columns=self.columns
            )
            conn.connection.commit()

        _logger.debug("Data successfully uploaded using copy")

    def upload_data(self,
                    data: Iterator[Dict[str, Any]],
                    replace_if_exists: Optional[bool] = False
                    ) -> None:
        """
        Upload dataFrame to database
        if table not exists it gets created by
        upload_with_pd method using sample of data
        ""and then it uses upload_with_copy to load the rest
        @param data: pd df or list of dicts
        @param replace_if_exists:
        @return: None
        """
        start = datetime.now()
        _logger.info(f"Uploading data to db\n"
                     f"schema_name: {self.schema}\n"
                     f"table_name: {self.table}\n"
                     f"replace_if_exists: {replace_if_exists}")

        data = assert_type_df(data)

        if not self.table_exists(self.table) or replace_if_exists:
            _logger.info("table don't exists or should be rewritten "
                         "-> using sample to create table")
            sample, data = split(data, self.sample)
            self.upload_with_pd(sample, replace_if_exists)

        if not data.empty:
            self.upload_with_copy(data, self.table)

        _logger.info(f"Data successfully uploaded, "
                     f"rows: {len(data)} | "
                     f"took: {datetime.now() - start} ")

    def _upsert(self, conflict_columns: list) -> None:
        """
        Do upsert. execute all commits in one conn
        @param conflict_columns:
        @return:
        """
        update_stmt, insert_stmt = self.build_u_i_queries(conflict_columns)

        with self.database.engine.begin() as con:
            save_point = con.begin_nested()
            _logger.info(f"Updating table {self.table}"
                         f" with {self.upsert_tmp_table}")
            con.execute(update_stmt)

            _logger.info(f"Inserting to table {self.table}"
                         f" from {self.upsert_tmp_table}")
            con.execute(insert_stmt)

            _logger.info(f"Upsert SUCCESS -> dropping tmp table "
                         f"{self.upsert_tmp_table}")
            con.execute(f"""
                        drop table if exists
                        {self.schema}.{self.upsert_tmp_table};
                        """
                        )
            save_point.commit()

    def upsert_data(self, data: Iterator[Dict[str, Any]],
                    conflict_columns: list
                    ) -> None:
        """
        update existing rows and insert non-existing rows
        must be done with two separate calls because
        there could be no unique constrains in the table
        @param data: df
        @param conflict_columns: on conflict columns
        @return:
        """
        start = datetime.now()
        _logger.info(f"Upserting data to db...\n"
                     f"schema_name: {self.schema}\n"
                     f"table_name: {self.table}\n"
                     f"conflict columns: {conflict_columns}"
                     )

        if not self.table_exists(self.table):
            _logger.info("table don't exists -> upload only ")
            return self.upload_data(data, self.table)

        self.create_tmp_table(conflict_columns)
        self.upload_with_copy(data, self.upsert_tmp_table)
        self._upsert(conflict_columns)

        _logger.info(f"Data successfully uploaded, "
                     f"rows: {len(data)} | "
                     f"took: {datetime.now() - start} ")
