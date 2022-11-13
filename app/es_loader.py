import logging
import json

from elasticsearch import Elasticsearch, helpers


from table import ElasticIndex

PAGE_SIZE = 500


class ESLoader:
    def __init__(self):
        self.index = 'index'

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
    def generate_actions(list_of_data: list):

        record = None
        for elem in list_of_data:
            record = {
                "id": elem['film_id'],
                "imdb_rating": elem['rating'],
                "genre": elem['genres'],
                "title": elem['title'],
                "description": elem['description'],
                "director": [elem['directors'][0]['name']] if elem['directors'] else [],
                "actors_names": [actor['name'] for actor in elem['actors']],
                "writers_names": [writer['name'] for writer in elem['writers']],
                "actors": elem['actors'],
                "writers": elem['writers']
            }
            yield {"_id": elem['film_id'], "_source": dict(ElasticIndex(**record))}

    def create_or_update_record(self, record: list):

        resp = helpers.bulk(
                            client=self.es,
                            index=self.index,
                            actions=self.generate_actions(record),
                            # chunk_size=PAGE_SIZE
        )
        # resp = self.es.index(self.index, id=record.id, body=dict(record))
        # print(resp['result'])


def main():
    es = ESLoader()

    record = {
        "id": "0c63fc65-3fc2-48f3-875e-9e373f251d3c",
        "imdb_rating": 3.0,
        "genre": ['genre1', 'genre2'],
        "title": "Glow Up: Britain's Next Make-Up Star",
        "description": "A talented group of aspiring make-up artists attempt to prove their potential to industry professionals in this competition hosted by Stacey Dooley.",
        "director": "Peter Demetris",
        "actors_names": ['Actor1', 'Actor2'],
        "writers_names": ['Wr1', 'Wr2'],
        "actors": [{'id': '1', 'name': 'Actor1'}, {'id': '2', 'name': 'Actor2'}],
        "writers": [{'id': '1', 'name': 'Wr1'}, {'id': '2', 'name': 'Wr2'}]
    }

    es.create_or_update_record(ElasticIndex(**record))


if __name__ == '__main__':
    main()


