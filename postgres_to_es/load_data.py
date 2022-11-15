"""
Yandex.Practicum sprint 1.
Main section

Author: Petr Schislyaev
Date: 14/11/2022
"""

import contextlib
import os
import psycopg2

from dotenv import load_dotenv
from redis import Redis

from postgresextractor import PostgresExtractor
from es_loader import ESLoader
from redis_storage import RedisStorage, State


def main():
    """Connect to SQLite and Postgres, extract from sqlite and load to postgres."""
    load_dotenv('../app/config/.env')

    db_name = os.environ.get('PG_DB_NAME')
    user = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    host = os.environ.get('DB_HOST')
    port = os.environ.get('DB_PORT')
    PAGE_SIZE = int(os.environ.get('PAGE_SIZE'))

    dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': host, 'port': port}
    es = ESLoader()

    adapter = Redis(
        host='127.0.0.1',
        port=6379,
        db=0,
        password=None,
        socket_timeout=None,
        decode_responses=True
    )
    storage = RedisStorage(adapter)
    state = State(storage)

    with contextlib.closing(psycopg2.connect(**dsl)) as pg_conn:
        pg = PostgresExtractor(pg_conn, state)
        pg.extract()
        i = 0
        while data := pg.cursor.fetchmany(PAGE_SIZE):
            res = pg.transform(data)
            es.create_or_update_record(res)
            i += 1
            print(i)


if __name__ == '__main__':
    main()
