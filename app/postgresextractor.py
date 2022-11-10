"""
Yandex.Practicum sprint 1.

Saving to Postgres logic

Author: Petr Schislyaev
Date: 16/10/2023
"""
import logging
from psycopg2 import Error
from psycopg2.extensions import connection as _connection


PAGE_SIZE = 10


class PostgresExtractor:

    def __init__(self, conn: _connection):
        """
        Organize input fields through pydentic mechanics.

        Args:
            conn: _connection
        """
        self.conn = conn
        self.cursor = conn.cursor()

    def extract(self):
        """
        film_work table.

        All the other table follow the same logic

        Args:
            data: table list
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
            return self.cursor.fetchmany(PAGE_SIZE)

        # Using special error handler from psycopg
        except (Exception, Error) as error:
            logging.exception(error)


    def transform(self, data):

        res = {
               'film_id': data[0],
               'title': data[1],
               'description': data[2],
               'rating': data[3],
               'type': data[4],
               'created': data[5],
               'modified': data[6],
               'name_role': [{i['person_name']: i['person_role']} for i in data[7]],
               'genres': [k for k in data[8]]
        }
