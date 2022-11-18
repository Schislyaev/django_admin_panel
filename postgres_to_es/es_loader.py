import json
import config

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, ElasticsearchException, helpers
from table import ElasticIndex
from util import log
from redis_storage import State

load_dotenv('../app/config/.env')
PAGE_SIZE = config.PAGE_SIZE
INDEX_NAME = config.INDEX_NAME
HOST = config.HOST_LOCAL
PORT = config.ELASTIC_PORT

# logging setup
logger = log(__name__)


class ESLoader:
    def __init__(self):
        self.index = INDEX_NAME

        try:
            with open('film_index.json') as file:
                self.settings = json.load(file)
        except Exception as er:
            logger.exception(er)
            logger.error('Не удалось загрузить схему для ES')
            raise er

        self.es = self.connect_elasticsearch()
        self.create_index()

    @staticmethod
    def connect_elasticsearch():
        _es = None
        try:
            _es = Elasticsearch([{'host': HOST, 'port': PORT}])
            if _es.ping():
                logger.info('Connected elastic')
            else:
                raise ElasticsearchException
        except ElasticsearchException as er:
            logger.exception(er)
            raise er
        return _es

    def create_index(self) -> bool:
        created = False

        try:
            if not self.es.indices.exists(self.index):
                self.es.indices.create(index=self.index, ignore=400, body=self.settings)
                logger.info('Index created')
                created = True
            else:
                logger.info('Already exists')
        except ElasticsearchException as er:
            logger.exception(er)
        return created

    @staticmethod
    def generate_actions(list_of_data: list[ElasticIndex]) -> dict:
        """Func for bulk process."""

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

    def load(self, state: State, record: list):

        try:
            resp = helpers.bulk(
                client=self.es,
                index=self.index,
                actions=self.generate_actions(record),
            )

            # отслеживаем дату - кладем ее в редис
            state.set_state('modified', record[-1].modified)
            logger.info(f'Сохранили в Redis состояние {state.get_state("modified")}')
        except Exception as er:
            logger.exception(er)
            logger.error('Не удалось загрузить данные в ES')

    def close(self):
        """Func for correct closing."""
        try:
            self.es.transport.connection_pool.close()
            logger.info('Соединение с ES закрыто')
        except Exception as er:
            logger.exception(er)
            logger.error('Проблема с закрытием соединения ES')
