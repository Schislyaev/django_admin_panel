import config
import os

from dotenv import load_dotenv
from psycopg2 import Error, extras
from psycopg2.extensions import connection as _connection
from redis_storage import State
from table import ElasticIndex
from util import postgres_main_query, log

load_dotenv('../app/config/.env')
PAGE_SIZE = config.PAGE_SIZE

# logging setup
logger = log(__name__)


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

        self.main_query = os.environ.get('PG_MAIN_QUERY')

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

            # Уменьшаю минимальное время на одну миллисекунду, что бы учесть оригинальное минимальное время при
            # строгой выборке
            modified_date = modified_date if modified_date else min_modified_date[0]\
                .replace(microsecond=min_modified_date[0].microsecond - 1)

            postgres_main_query(self.cursor, modified_date)

        # Using special error handler from psycopg
        except (Exception, Error) as error:
            logger.exception(error)

    @staticmethod
    def transform(data: list) -> list:

        res = []
        last_modified = str()
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
                writers=role_list('writer'),
                modified=str(elem['modified'])
            )

            res.append(pydantic_transform)

        return res

