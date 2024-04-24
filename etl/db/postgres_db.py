import psycopg2
import psycopg2.sql
from psycopg2.extras import RealDictCursor, RealDictRow

from configs.logger import etl_logger
from utils import backoff


class PostgresConnection(object):
    """ Postgres connection class. """

    @backoff(start_sleep_time=1)
    def _connect(self) -> None:
        """ Postgres connection  method. """

        self.connection = psycopg2.connect(**self.postgres_settings)
        self.connection.set_session(readonly=True)

        etl_logger.info(f"Connection to the database '{self.postgres_settings['dbname']}' is established.")

    def __init__(self, postgres_settings: dict) -> None:
        """ Postgres connection constructor. """

        self.postgres_settings = postgres_settings
        self._connect()

    # посыл ясен, спасибо :) так как в этой конкретной реализации у меня нет контекстного менеджера для
    # connection - просто удалила __del__, потому что соединение мне еще будет нужно. в целом мне показалось
    # что выбрала не самый удачный вариант реализации класса (пока вообще кажется что без класса проще),
    # надо будет еще покопать эту тему.

    @backoff(start_sleep_time=0.3)
    def retry_fetchall(self, sql: psycopg2.sql.Composed, **kwargs) -> list[RealDictRow]:
        """ Query executor. """

        etl_logger.info("Execution of query.")

        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, kwargs)

            return cursor.fetchall()
