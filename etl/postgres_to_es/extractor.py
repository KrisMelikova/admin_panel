from datetime import datetime
from typing import Callable

from psycopg2.sql import SQL, Identifier

from etl.configs.etl_config import settings
from etl.configs.logger import etl_logger
from etl.db.postgres_db import PostgresConnection
from etl.db.queries import QueryGenerator
from etl.storage.state_storage import JsonFileStorage, State


class Extractor(object):
    """ Extract data from PostgreSQL DB. """

    def __init__(self, postgres: PostgresConnection, result_handler: Callable) -> None:
        """ Extractor class constructor. """

        self.postgres = postgres
        self.result_handler = result_handler
        self.storage = JsonFileStorage(settings.storage_file_path)
        self.state = State(self.storage)

    @staticmethod
    def _extract_record_ids(query_result: list) -> set:
        return set(el['id'] for el in query_result)

    def proccess(self, table: str, schema: str, page_size: int, updated_at: str) -> None:
        """ Get information about modified filmworks."""

        filmwork_ids = set()
        query_generator = QueryGenerator(schema, updated_at)

        persons_query = query_generator.generate_persons_query()
        prepared_persons_query = SQL(persons_query).format(table=Identifier(schema, table))

        p_query_result = self.postgres.retry_fetchall(prepared_persons_query, modified=updated_at, page_size=page_size)
        if p_query_result:
            persons_ids = tuple(i["id"] for i in p_query_result)
            persons_filmwork_query = query_generator.generate_person_filmwork_query(persons_ids)
            prepared_person_filmwork_query = SQL(persons_filmwork_query).format(table=Identifier(schema, table))
            p_f_query_result = self.postgres.retry_fetchall(
                prepared_person_filmwork_query, modified=updated_at, page_size=page_size,
            )
            filmwork_person_ids = self._extract_record_ids(p_f_query_result)
            filmwork_ids.update(filmwork_person_ids)

        genres_query = query_generator.generate_genre_query()
        prepared_genres_query = SQL(genres_query).format(table=Identifier(schema, table))

        g_query_result = self.postgres.retry_fetchall(prepared_genres_query, modified=updated_at, page_size=page_size)
        if g_query_result:
            genres_ids = tuple(i["id"] for i in p_query_result)
            genres_filmwork_query = query_generator.generate_person_filmwork_query(genres_ids)
            prepared_genres_filmwork_query = SQL(genres_filmwork_query).format(table=Identifier(schema, table))
            g_f_query_result = self.postgres.retry_fetchall(
                prepared_genres_filmwork_query, modified=updated_at, page_size=page_size,
            )
            filmwork_person_ids = self._extract_record_ids(g_f_query_result)
            filmwork_ids.update(filmwork_person_ids)

        if filmwork_ids:
            filmworks_query = query_generator.generate_filmwork_query(tuple(filmwork_ids))
            prepared_filmworks_query = SQL(filmworks_query).format(table=Identifier(schema, table))

            filmworks_query_result = self.postgres.retry_fetchall(
                prepared_filmworks_query, modified=updated_at, page_size=page_size,
            )

            etl_logger.info(f"Count of updated elements is {len(filmworks_query_result)}.")

            self.state.set_state('updated_at', str(datetime.utcnow()))

            if filmworks_query_result:
                self.result_handler(filmworks_query_result)
