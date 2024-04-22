from elasticsearch import Elasticsearch, helpers

from configs.logger import etl_logger
from utils import backoff


class Loader(object):
    """ Elastic Search loader. """

    def __init__(self, es: Elasticsearch, index: str, index_schema: dict = None) -> None:
        """ Elastic Search loader class constructor. """

        self.es = es
        self.index = index

        if index_schema and not self.es.indices.exists(index=index):
            self.create_index(index=index, index_schema=index_schema)

    @backoff(start_sleep_time=0.3)
    def create_index(self, index: str, index_schema: dict) -> None:
        """ Creates index. """

        self.es.indices.create(index=index, body=index_schema)

    @backoff(start_sleep_time=0.3)
    def process(self, movies_data: dict) -> None:
        """ Load data to Elasticsearch. """

        etl_logger.info(f"Adding {len(movies_data)} movies to ES.")

        actions = [
            {
                "_index": self.index,
                "_id": el.id,
                "_source": el.dict(),
            }
            for el in movies_data
        ]

        _, errors = helpers.bulk(self.es, actions, stats_only=False)
        if errors:
            etl_logger.error(f"Error while loading data to ES: {errors}")
