"""
Yandex.Practicum sprint 3.
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
from psycopg2.extensions import connection
from dotenv import load_dotenv
from redis import Redis, ConnectionError
from elasticsearch import ElasticsearchException

from postgresextractor import PostgresExtractor
from es_loader import ESLoader
from redis_storage import RedisStorage, State
from util import sleep

# logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(f"logs/{__name__}.log", mode='w')
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

handler.setFormatter(formatter)
logger.addHandler(handler)

timeout = int(os.environ.get('TIMEOUT'))    # Время повтора сервиса в секундах


class ETL:
    def __init__(self):
        load_dotenv('../app/config/.env')

        db_name = os.environ.get('PG_DB_NAME')
        user = os.environ.get('DB_USER')
        password = os.environ.get('DB_PASSWORD')
        self.host_local = os.environ.get('DB_HOST_LOCAL')
        port = os.environ.get('DB_PORT')
        self.PAGE_SIZE = int(os.environ.get('PAGE_SIZE'))

        self.dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': self.host_local, 'port': port}

        self.state = self.redis_connect()

    @backoff.on_exception(
        backoff.expo,
        ConnectionError,
        logger=logger
    )
    def redis_connect(self) -> State | Exception:
        try:
            adapter = Redis(
                host=self.host_local,
                port=6379,
                db=0,
                password=None,
                socket_timeout=None,
                decode_responses=True
            )
            storage = RedisStorage(adapter)
            state = State(storage)
            return state
        except ConnectionError as er:
            logger.exception(er)
            raise er

    @backoff.on_exception(
        backoff.expo,
        Error,
        logger=logger
    )
    def postgres_connect(self) -> connection | Exception:
        try:
            pg = psycopg2.connect(**self.dsl)
            return pg

        except Error as er:
            logger.exception(er)
            raise er

    @backoff.on_exception(
        backoff.expo,
        ElasticsearchException,
        logger=logger
    )
    def es_connect(self) -> ESLoader | Exception:
        try:
            es = ESLoader()
            return es
        except ElasticsearchException as er:
            logger.exception(er)
            raise er

    @sleep(timeout)
    def main(self):

        with contextlib.closing(self.postgres_connect()) as pg_conn, contextlib.closing(self.es_connect()) as es_conn:

            pg = PostgresExtractor(pg_conn, self.state)
            pg.extract()
            while data := pg.cursor.fetchmany(self.PAGE_SIZE):
                res = pg.transform(data)
                es_conn.load(res)


def main():

    etl = ETL()
    etl.main()


if __name__ == '__main__':
    main()
