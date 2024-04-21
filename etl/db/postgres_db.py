from typing import List

import psycopg2
import psycopg2.sql
from psycopg2.extras import RealDictCursor, RealDictRow

from etl.configs.logger import etl_logger


class PostgresConnection(object):
    """ Postgres connection class. """

    def _connect(self) -> None:
        """ Postgres connection  method. """

        self.connection = psycopg2.connect(**self.postgres_settings)
        self.connection.set_session(readonly=True)

        etl_logger.info(f"Connection to the database '{self.postgres_settings['dbname']}' is established.")

    def __init__(self, postgres_settings: dict) -> None:
        """ Postgres connection constructor. """

        self.postgres_settings = postgres_settings
        self._connect()

    def __del__(self) -> None:
        """ Close Postgres connection. """

        self.connection.close()
        etl_logger.info(f"Connection to the database {self.postgres_settings['dbname']} is closed.")

    def retry_fetchall(self, sql: psycopg2.sql.Composed, **kwargs) -> list[RealDictRow]:
        """ Query executor. """

        etl_logger.info(f"Execution of query.")

        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, kwargs)

            return cursor.fetchall()
