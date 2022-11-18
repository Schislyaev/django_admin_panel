"""
Yandex.Practicum sprint 3.
Main section

Author: Petr Schislyaev
Date: 14/11/2022
"""

import config
import contextlib
import os

import backoff
import psycopg2
from dotenv import load_dotenv
from elasticsearch import ElasticsearchException
from es_loader import ESLoader
from postgresextractor import PostgresExtractor
from psycopg2 import Error
from psycopg2.extensions import connection
from redis import ConnectionError, Redis
from redis_storage import RedisStorage, State
from util import sleep, log

# logging setup
logger = log(__name__)


TIMEOUT = config.TIMEOUT    # Время повтора сервиса в секундах


class ETL:
    def __init__(self):
        load_dotenv('../app/config/.env')

        self.host_local = config.HOST_LOCAL      # os.environ.get('DB_HOST_LOCAL')
        self.PAGE_SIZE = config.PAGE_SIZE       # int(os.environ.get('PAGE_SIZE'))

        self.dsl = {
            'dbname': os.environ.get('PG_DB_NAME'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD'),
            'host': self.host_local,
            'port': os.environ.get('DB_PORT')
        }

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
                port=config.REDIS_PORT,       # int(os.environ.get('REDIS_PORT')),
                db=config.REDIS_DB_NUMBER,     # int(os.environ.get('REDIS_DB_NUMBER')),
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

    @sleep(TIMEOUT)
    def main(self):

        with contextlib.closing(self.postgres_connect()) as pg_conn, contextlib.closing(self.es_connect()) as es_conn:

            pg = PostgresExtractor(pg_conn, self.state)
            pg.extract()
            while data := pg.cursor.fetchmany(self.PAGE_SIZE):
                res = pg.transform(data)
                es_conn.load(self.state, res)


def main():

    etl = ETL()
    etl.main()


if __name__ == '__main__':
    main()
