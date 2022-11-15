"""
Yandex.Practicum sprint 1.
Main section

Author: Petr Schislyaev
Date: 14/11/2022
"""

import contextlib
import logging
import os
import psycopg2
import backoff

from psycopg2 import Error
from dotenv import load_dotenv
from redis import Redis
from elasticsearch import ElasticsearchException

from postgresextractor import PostgresExtractor
from es_loader import ESLoader
from redis_storage import RedisStorage, State


class ETL:
    def __init__(self):
        load_dotenv('../app/config/.env')

        db_name = os.environ.get('PG_DB_NAME')
        user = os.environ.get('DB_USER')
        password = os.environ.get('DB_PASSWORD')
        host = os.environ.get('DB_HOST_LOCAL')
        port = os.environ.get('DB_PORT')
        self.PAGE_SIZE = int(os.environ.get('PAGE_SIZE'))

        self.dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': host, 'port': port}
        # self.es = ESLoader()

        adapter = Redis(
            host='127.0.0.1',
            port=6379,
            db=0,
            password=None,
            socket_timeout=None,
            decode_responses=True
        )
        storage = RedisStorage(adapter)
        self.state = State(storage)

    @backoff.on_exception(
        backoff.expo,
        Error
    )
    def postgres_connect(self):
        try:
            pg = psycopg2.connect(**self.dsl)

            return pg
        except Error as er:
            logging.error(er)
            raise er

    @backoff.on_exception(
        backoff.expo,
        ElasticsearchException
    )
    def es_connect(self):
        try:
            es = ESLoader()
            return es
        except ElasticsearchException as er:
            logging.error(er)
            raise er

    def main(self):

        with contextlib.closing(self.es_connect()) as es_conn, contextlib.closing(self.postgres_connect()) as pg_conn:

            pg = PostgresExtractor(pg_conn, self.state)
            pg.extract()
            i = 0
            while data := pg.cursor.fetchmany(self.PAGE_SIZE):
                res = pg.transform(data)
                es_conn.create_or_update_record(res)
                i += 1
                print(i)


def main():

    etl = ETL()
    etl.main()


if __name__ == '__main__':
    main()
