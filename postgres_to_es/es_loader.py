import logging
import json
import os

from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

from table import ElasticIndex

load_dotenv('../app/config/.env')
PAGE_SIZE = int(os.environ.get('PAGE_SIZE'))
INDEX_NAME = os.environ.get('INDEX_NAME')


class ESLoader:
    def __init__(self):
        self.index = INDEX_NAME

        with open('data.json') as file:
            self.settings = json.load(file)
        self.es = self.connect_elasticsearch()
        self.create_index()

    @staticmethod
    def connect_elasticsearch():
        _es = None
        _es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
        if _es.ping():
            print('Connected elastic')
        else:
            print('Could not connect elastic')
            logging.basicConfig(level=logging.ERROR)
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
        except Exception as er:
            print(str(er))
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

    def create_or_update_record(self, record: list):

        resp = helpers.bulk(
                            client=self.es,
                            index=self.index,
                            actions=self.generate_actions(record),
        )