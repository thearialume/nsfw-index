import psycopg
from psycopg import OperationalError
from psycopg.cursor import Cursor

from .settings import DATABASE_URL

_connection = None


def get_connection():
    global _connection

    if _connection is None or _connection.closed:
        _connection = psycopg.connect(DATABASE_URL)

    return _connection


def get_cursor() -> Cursor:
    global _connection
    try:
        conn = get_connection()
        return conn.cursor()
    except OperationalError:
        _connection = psycopg.connect(DATABASE_URL)
        return _connection.cursor()
