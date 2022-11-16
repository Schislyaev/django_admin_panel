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
from redis_storage import RedisStorage, State

load_dotenv('../app/config/.env')
PAGE_SIZE = int(os.environ.get('PAGE_SIZE'))

# logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(f"logs/{__name__}.log", mode='w')
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

handler.setFormatter(formatter)
logger.addHandler(handler)


class PostgresExtractor:

    def __init__(self, conn: _connection, state: State):
        """
        Organize input fields through pydentic mechanics.

        Args:
            conn: _connection
        """
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=extras.DictCursor)
        self.state = state

    def extract(self) -> None:
        """
        Extract from PG.

        Get data with condition of date

        Args:
            date: date to query
        """

        try:
            self.cursor.execute("""SELECT MIN(modified) FROM content.film_work""")
            min_modified_date = self.cursor.fetchone()
            modified_date = self.state.get_state('modified')
            # Уменьшаю минимальное время на одну миллисекунду, что бы учесть оригинальное минимальное время при строгой выборке
            modified_date = modified_date if modified_date else min_modified_date[0].replace(microsecond=min_modified_date[0].microsecond - 1)

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
                        WHERE fw.modified > '{modified_date}'
                        GROUP BY fw.id
                        ORDER BY fw.modified
                        """
            self.cursor.execute(query)

        # Using special error handler from psycopg
        except (Exception, Error) as error:
            logger.exception(error)

    def transform(self, data: list) -> list:

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

            # отслеживаем дату - кладем ее в редис
            self.state.set_state('modified', elem['modified'])

            res.append(pydantic_transform)

        return res

