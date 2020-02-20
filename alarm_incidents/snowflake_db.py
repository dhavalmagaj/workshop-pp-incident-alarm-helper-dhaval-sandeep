import os
from typing import Dict, List, Tuple, Union

from snowflake.connector import connect as sf_connect
from snowflake.connector.cursor import DictCursor, SnowflakeCursor


class SnowFlakeDb(object):
    """Represent SnowFlake database"""

    def __init__(self, username: str = None, password: str = None):
        self._sf_config = {
            "account": os.getenv("SF_ACCOUNT") or "gx82091",
            "warehouse": os.getenv("SF_WAREHOUSE") or "DYNAMO_QUERIES",
            "role": os.getenv("SF_ROLE") or "READ_ONLY_DATA_ROLE",
            "user": username or os.getenv("SF_USER"),
            "password": password or os.getenv("SF_PASS"),
            "database": "DL",
            "schema": "TELEMETRY",
            "paramstyle": "numeric",
        }

    def execute(self, query: str, params: Tuple = None, dict_cursor=False) -> None:
        """Execute query on SnowFlake database"""

        cursor_class = DictCursor if dict_cursor else SnowflakeCursor
        with sf_connect(**self._sf_config) as conn:
            with conn.cursor(cursor_class) as cursor:
                cursor.execute(query, params)

    def fetchall(
        self, query: str, params: Tuple = None, dict_cursor=False
    ) -> List[Union[Tuple, Dict]]:
        """Fetch all using given query from SnowFlake database"""

        cursor_class = DictCursor if dict_cursor else SnowflakeCursor
        with sf_connect(**self._sf_config) as conn:
            with conn.cursor(cursor_class) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
        return results

    def fetchone(
        self, query: str, params: Tuple = None, dict_cursor=False
    ) -> Union[Tuple, Dict]:
        """Fetch one using given query from SnowFlake database"""

        cursor_class = DictCursor if dict_cursor else SnowflakeCursor
        with sf_connect(**self._sf_config) as conn:
            with conn.cursor(cursor_class) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchone()
        return results
