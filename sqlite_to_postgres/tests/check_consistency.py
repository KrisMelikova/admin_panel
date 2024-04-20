import datetime
import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from sqlite_to_postgres.utils import sqlite_conn_context, sqlite_curs_context

load_dotenv()

SQLITE_DB_PATH_TEST = os.environ.get("SQLITE_DB_PATH_TEST")

PG_DSL = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
}

tables_mapping = {
    "content.film_work": "film_work",
    "content.genre": "genre",
    "content.genre_film_work": "genre_film_work",
    "content.person": "person",
    "content.person_film_work": "person_film_work",
}

tables_cols_mapping = {
    "film_work": "*",
    "genre": "*",
    "genre_film_work": "id, film_work_id, genre_id, created",
    "person": "*",
    "person_film_work": "id, film_work_id, person_id, role, created",
}


def test_count_of_tables_records():
    with (sqlite_conn_context(SQLITE_DB_PATH_TEST) as sqlite_conn,
          psycopg2.connect(**PG_DSL, cursor_factory=DictCursor) as pg_conn):
        with sqlite_curs_context(sqlite_conn) as sqlite_cursor, pg_conn.cursor() as pg_cursor:
            for pg_table, sq_table in tables_mapping.items():
                pg_cursor.execute("""SELECT COUNT(*) FROM %s;""" % pg_table)
                pg_data = pg_cursor.fetchall()[0][0]

                sqlite_cursor.execute("""SELECT COUNT(*) FROM %s;""" % sq_table)
                sq_data = sqlite_cursor.fetchall()[0][0]

                assert pg_data == sq_data


def test_content_of_tables():  # noqa CCR001
    with (sqlite_conn_context(SQLITE_DB_PATH_TEST) as sqlite_conn,
          psycopg2.connect(**PG_DSL, cursor_factory=DictCursor) as pg_conn):
        with sqlite_curs_context(sqlite_conn) as sqlite_cursor, pg_conn.cursor() as pg_cursor:
            for pg_table, sq_table in tables_mapping.items():
                pg_cursor.execute("""SELECT %s FROM %s order by id asc;""" % (tables_cols_mapping[sq_table], pg_table))
                pg_data = pg_cursor.fetchall()

                if sq_table == "film_work":
                    sqlite_cursor.execute(
                        "SELECT id, title, description, creation_date, rating, type, created_at, updated_at FROM "
                        "film_work order by id asc;")
                else:
                    sqlite_cursor.execute("""SELECT * FROM %s order by id asc;""" % sq_table)
                sq_data = sqlite_cursor.fetchall()

                for r_num, pg_row in enumerate(pg_data):
                    for i, pg_col in enumerate(pg_row):
                        sg_col = sq_data[r_num][i]
                        if type(pg_col) == datetime.datetime:
                            pg_col = pg_col.strftime("%Y-%m-%d %H:%M:%S.%f")
                            sg_col = datetime.datetime.strptime(sg_col, "%Y-%m-%d %H:%M:%S.%f+00").strftime(
                                "%Y-%m-%d %H:%M:%S.%f")

                        assert pg_col == sg_col
