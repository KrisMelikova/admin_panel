from datetime import datetime
from typing import Callable

from psycopg2.sql import SQL, Identifier

from configs.etl_config import settings
from configs.logger import etl_logger
from db.postgres_db import PostgresConnection
from db.queries import QueryGenerator
from storage.state_storage import JsonFileStorage, State


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
        return {el['id'] for el in query_result}

    def get_genres_data(
            self,
            query_generator: QueryGenerator,
            schema: str,
            table: str,
            updated_at: str,
            page_size: int,
    ) -> list:
        """ Get genres data from DB. """

        genres_query = query_generator.generate_genre_query()
        prepared_genres_query = SQL(genres_query).format(table=Identifier(schema, table))

        g_query_result = self.postgres.retry_fetchall(
            prepared_genres_query, modified=updated_at, page_size=page_size,
        )

        return g_query_result

    def get_persons_data(
            self,
            query_generator: QueryGenerator,
            schema: str,
            table: str,
            updated_at: str,
            page_size: int,
    ) -> list:
        """ Get persons data from DB. """

        persons_query = query_generator.generate_persons_query()
        prepared_persons_query = SQL(persons_query).format(table=Identifier(schema, table))

        p_query_result = self.postgres.retry_fetchall(
            prepared_persons_query, modified=updated_at, page_size=page_size,
        )

        return p_query_result

    def get_filmworks_data(
            self,
            query_generator: QueryGenerator,
            schema: str,
            table: str,
            updated_at: str,
            page_size: int,
            filmwork_ids: set,
    ) -> list:
        """ Get filmworks data from DB. """

        filmworks_query = query_generator.generate_filmwork_query(tuple(filmwork_ids))
        prepared_filmworks_query = SQL(filmworks_query).format(table=Identifier(schema, table))

        filmworks_query_result = self.postgres.retry_fetchall(
            prepared_filmworks_query, modified=updated_at, page_size=page_size,
        )

        return filmworks_query_result

    def proccess(self, table: str, schema: str, page_size: int, updated_at: str) -> None:
        """ Get information about modified filmworks."""

        self.state.set_state("updated_at", str(datetime.utcnow()))

        query_generator = QueryGenerator(schema, updated_at)
        extractor_result = {}

        if table == "genre":
            g_query_result = self.get_genres_data(query_generator, schema, table, updated_at, page_size)

            etl_logger.info(f"Count of updated genres is {len(g_query_result)}.")

            if g_query_result:
                extractor_result |= {"table": table, "data": g_query_result}
                self.result_handler(extractor_result)

        elif table == "person":
            p_query_result = self.get_persons_data(query_generator, schema, table, updated_at, page_size)

            etl_logger.info(f"Count of updated persons is {len(p_query_result)}.")

            if p_query_result:
                extractor_result |= {"table": table, "data": p_query_result}
                self.result_handler(extractor_result)

        elif table == "film_work":
            filmwork_ids = set()

            p_query_result = self.get_persons_data(query_generator, schema, table, updated_at, page_size)
            if p_query_result:
                persons_ids = tuple(i["id"] for i in p_query_result)

                persons_filmwork_query = query_generator.generate_person_filmwork_query(persons_ids)
                prepared_person_filmwork_query = SQL(persons_filmwork_query).format(
                    table=Identifier(schema, table),
                )
                p_f_query_result = self.postgres.retry_fetchall(
                    prepared_person_filmwork_query, modified=updated_at, page_size=page_size,
                )
                filmwork_person_ids = self._extract_record_ids(p_f_query_result)

                filmwork_ids.update(filmwork_person_ids)

            g_query_result = self.get_genres_data(query_generator, schema, table, updated_at, page_size)
            if g_query_result:
                genres_ids = tuple(i["id"] for i in p_query_result)

                genres_filmwork_query = query_generator.generate_person_filmwork_query(genres_ids)
                prepared_genres_filmwork_query = SQL(genres_filmwork_query).format(
                    table=Identifier(schema, table),
                )
                g_f_query_result = self.postgres.retry_fetchall(
                    prepared_genres_filmwork_query, modified=updated_at, page_size=page_size,
                )
                filmwork_person_ids = self._extract_record_ids(g_f_query_result)

                filmwork_ids.update(filmwork_person_ids)

            filmworks_query_result = self.get_filmworks_data(
                query_generator, schema, table, updated_at, page_size, filmwork_ids,
            )

            etl_logger.info(f"Count of updated filmworks is {len(filmworks_query_result)}.")

            if filmworks_query_result:
                extractor_result |= {"table": table, "data": filmworks_query_result}
                self.result_handler(extractor_result)
