import logging
import json
import os

from elasticsearch import Elasticsearch, helpers, ElasticsearchException
from dotenv import load_dotenv

from table import ElasticIndex

load_dotenv('../app/config/.env')
PAGE_SIZE = int(os.environ.get('PAGE_SIZE'))
INDEX_NAME = os.environ.get('INDEX_NAME')
HOST = os.environ.get('DB_HOST_LOCAL')

# logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

handler = logging.FileHandler(f"logs/{__name__}.log", mode='w')
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

handler.setFormatter(formatter)
logger.addHandler(handler)


class ESLoader:
    def __init__(self):
        self.index = INDEX_NAME

        try:
            with open('data.json') as file:
                self.settings = json.load(file)
        except Exception as er:
            logger.exception(er)

        self.es = self.connect_elasticsearch()
        self.create_index()

    @staticmethod
    def connect_elasticsearch():
        _es = None
        try:
            _es = Elasticsearch([{'host': HOST, 'port': 9200}])
            if _es.ping():
                print('Connected elastic')
            else:
                raise ElasticsearchException
        except ElasticsearchException as er:
            logger.exception(er)
            raise er
        return _es

    def create_index(self):
        created = False

        try:
            if not self.es.indices.exists(self.index):
                self.es.indices.create(index=self.index, ignore=400, body=self.settings)
                print('Index created')
                created = True
            else:
                print('Already exists')
        except ElasticsearchException as er:
            logger.exception(er)
        finally:
            return created

    @staticmethod
    def generate_actions(list_of_data: list[ElasticIndex]):

        for elem in list_of_data:
            record = {
                "id": elem.id,
                "imdb_rating": elem.imdb_rating,
                "genre": elem.genre,
                "title": elem.title,
                "description": elem.description,
                "director": elem.director,
                "actors_names": elem.actors_names,
                "writers_names": elem.writers_names,
                "actors": elem.actors,
                "writers": elem.writers
            }
            yield {"_id": elem.id, "_source": record}

    def load(self, record: list):

        resp = helpers.bulk(
                            client=self.es,
                            index=self.index,
                            actions=self.generate_actions(record),
        )

    def close(self):
        self.es.transport.connection_pool.close()
