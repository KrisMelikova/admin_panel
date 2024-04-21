from elasticsearch import Elasticsearch, helpers

from etl.configs.logger import etl_logger


class Loader(object):
    """ Elastic Search loader. """

    def __init__(self, es: Elasticsearch, index: str, index_schema: dict = None) -> None:
        """ Elastic Search loader class constructor. """

        self.es = es
        self.index = index

        if index_schema and not self.es.indices.exists(index=index):
            self.create_index(index=index, index_schema=index_schema)

    def create_index(self, index: str, index_schema: dict) -> None:
        """ Creates index. """

        self.es.indices.create(index=index, body=index_schema)

    def process(self, movies_data: dict) -> None:
        """ Load data to Elasticsearch. """

        etl_logger.info(f"Adding {len(movies_data)} movies to ES.")

        actions = [
            {
                "_index": self.index,
                "_id": el.id,
                "_source": el.dict()
            }
            for el in movies_data
        ]


        _, errors = helpers.bulk(self.es, actions, stats_only=False)

        res = self.es.search(index="movies", body={
            'size': 100,
            'query': {
                'match_all': {}
            }
        })

        #
        #
        # data_w_bulk_format = list(map(self.convert_to_bulk_format, movies_data))
        #
        # _, errors = helpers.bulk(self.es, data_w_bulk_format, stats_only=False)

        import pdb;
        pdb.set_trace()
