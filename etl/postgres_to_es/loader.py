from elasticsearch import Elasticsearch

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

    def proccess(self, data: dict) -> None:
        """ Load data to Elasticsearch. """

        import pdb;pdb.set_trace()
