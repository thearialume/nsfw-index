import psycopg

from .settings import DATABASE_URL

connection = psycopg.connect(DATABASE_URL)
cursor = connection.cursor()