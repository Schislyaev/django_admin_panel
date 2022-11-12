"""
Yandex.Practicum sprint 1.
Main section

Author: Petr Schislyaev
Date: 16/10/2023
"""

import contextlib
import os

import psycopg2
from dotenv import load_dotenv

from postgresextractor import PostgresExtractor


DATA_TABLES = ('film_work', 'genre', 'person', 'genre_film_work', 'person_film_work')


def main():
    """Connect to SQLite and Postgres, extract from sqlite and load to postgres."""
    load_dotenv('config/.env')

    db_name = os.environ.get('PG_DB_NAME')
    user = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    host = os.environ.get('DB_HOST')
    port = os.environ.get('DB_PORT')

    dsl = {'dbname': db_name, 'user': user, 'password': password, 'host': host, 'port': port}
    with contextlib.closing(psycopg2.connect(**dsl)) as pg_conn:
        pg = PostgresExtractor(pg_conn)
        res = pg.extract()[0]
        res = pg.transform(res)
        print(res)


if __name__ == '__main__':
    main()
