from elasticsearch import Elasticsearch, helpers

from configs.logger import etl_logger
from es.index_schema import genres, movies, persons
from utils import backoff


class Loader(object):
    """ Elastic Search loader. """

    def __init__(self, es: Elasticsearch) -> None:
        """ Elastic Search loader class constructor. """

        self.es = es

    @backoff(start_sleep_time=0.3)
    def create_index(self, index: str, index_schema: dict) -> None:
        """ Creates index. """

        self.es.indices.create(index=index, body=index_schema)

    @backoff(start_sleep_time=0.3)
    def process(self, transformed_result: dict) -> None:
        """ Load data to Elasticsearch. """

        table = transformed_result.get("table")
        transformed_data = transformed_result.get("data")

        index = None
        index_schema = None

        if table == "genre":
            etl_logger.info(f"Adding {len(transformed_data)} genres to ES.")

            index = "genres"
            index_schema = genres

        elif table == "person":
            etl_logger.info(f"Adding {len(transformed_data)} persons to ES.")

            index = "persons"
            index_schema = persons

        elif table == "film_work":
            etl_logger.info(f"Adding {len(transformed_data)} filmworks to ES.")

            index = "movies"
            index_schema = movies

        if index_schema and not self.es.indices.exists(index=index):
            self.create_index(index=index, index_schema=index_schema)

        actions = [
            {
                "_index": index,
                "_id": el.id,
                "_source": el.dict(),
            }
            for el in transformed_data
        ]

        _, errors = helpers.bulk(self.es, actions, stats_only=False)
        if errors:
            etl_logger.error(f"Error while loading data to ES: {errors}")
