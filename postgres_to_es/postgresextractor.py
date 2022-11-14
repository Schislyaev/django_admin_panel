"""
Yandex.Practicum sprint 3.

Saving to Postgres logic

Author: Petr Schislyaev
Date: 14/11/2022
"""
import logging
import os
from psycopg2 import Error, extras
from psycopg2.extensions import connection as _connection
from dotenv import load_dotenv

from table import ElasticIndex

load_dotenv('../app/config/.env')
PAGE_SIZE = int(os.environ.get('PAGE_SIZE'))


class PostgresExtractor:

    def __init__(self, conn: _connection):
        """
        Organize input fields through pydentic mechanics.

        Args:
            conn: _connection
        """
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=extras.DictCursor)

    def extract(self, state):
        """
        Extract from PG.

        Get data with condition of state

        Args:
            state: table list
        """

        try:

            query = f"""
                        SELECT
                           fw.id,
                           fw.title,
                           fw.description,
                           fw.rating,
                           fw.type,
                           fw.created,
                           fw.modified,
                           COALESCE (
                               json_agg(
                                   DISTINCT jsonb_build_object(
                                       'person_role', pfw.role,
                                       'person_id', p.id,
                                       'person_name', p.full_name
                                   )
                               ) FILTER (WHERE p.id is not null),
                               '[]'
                           ) as persons,
                           array_agg(DISTINCT g.name) as genres
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person p ON p.id = pfw.person_id
                        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre g ON g.id = gfw.genre_id
--                         WHERE fw.modified > '<время>'
                        GROUP BY fw.id
                        ORDER BY fw.modified 
                        """
            self.cursor.execute(query)
            # return self.cursor.fetchmany(PAGE_SIZE)

        # Using special error handler from psycopg
        except (Exception, Error) as error:
            logging.exception(error)

    # def fetch_data(self):
    #     while data := self.cursor.fetchmany(PAGE_SIZE):
    #         yield data

    def transform(self, data):

        res = []
        for elem in data:

            role_list = lambda role: [{'id': k.get('person_id'), 'name': k.get('person_name')} for k in elem['persons']
                                      if k.get('person_role') == role]

            pydantic_transform = ElasticIndex(
                id=elem['id'],
                imdb_rating=elem['rating'],
                genre=[k for k in elem['genres']],
                title=elem['title'],
                description=elem['description'],
                director=[director['name'] for director in role_list('director')],
                actors_names=[actor['name'] for actor in role_list('actor')],
                writers_names=[writer['name'] for writer in role_list('writer')],
                actors=role_list('actor'),
                writers=role_list('writer')
            )

            # res.append({
            #         'film_id': elem['id'],
            #         'title': elem['title'],
            #         'description': elem['description'],
            #         'rating': elem['rating'],
            #         'type': elem['type'],
            #         'created': elem['created'],
            #         'modified': elem['modified'],
            #         'actors': role_list('actor'),
            #         'directors': role_list('director'),
            #         'writers': role_list('writer'),
            #         'genres': [k for k in elem['genres']]
            # })

            res.append(pydantic_transform)

        return res
