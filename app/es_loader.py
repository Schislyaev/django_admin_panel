import logging
import json
from elasticsearch import Elasticsearch


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    if _es.ping():
        print('Connected elastic')
    else:
        print('Could not connect elastic')
        logging.basicConfig(level=logging.ERROR)
    return _es


def create_index(es_object, index_name='index'):
    created = False
    with open('data.json') as file:
        settings = json.load(file)

    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Index created')
            created = True
        else:
            print('Already exists')
    except Exception as er:
        print(str(er))
    finally:
        return created


def main():
    es = connect_elasticsearch()
    create_index(es)


if __name__ == '__main__':
    main()


