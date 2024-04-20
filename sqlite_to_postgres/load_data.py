import dataclasses
# os используется ниже, этот импорт необходим
import os
import sqlite3
from dataclasses import astuple, fields

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import DictCursor

from dataclasses_sqlite_to_pg import Genre, GenreMovie, Movie, Person, PersonMovie
from utils import sqlite_conn_context, sqlite_curs_context

load_dotenv()

dataclass_sqlite_tables_mapping = {
    Movie: "film_work",
    Genre: "genre",
    GenreMovie: "genre_film_work",
    Person: "person",
    PersonMovie: "person_film_work",
}

dataclass_pg_tables_mapping = {
    Movie: "content.film_work",
    Genre: "content.genre",
    GenreMovie: "content.genre_film_work",
    Person: "content.person",
    PersonMovie: "content.person_film_work",
}


def load_data_from_sqlite(sqlite_conn: sqlite3.Connection, dt: dataclasses, table: str):
    """ Загрузка данных из SQLite. """

    with sqlite_curs_context(sqlite_conn) as sqlite_cursor:
        try:
            fields_names: list = [field.name for field in fields(dt)]
            fields_names_prepared: str = ", ".join(fields_names)

            query: str = f"SELECT {fields_names_prepared} FROM {table}"

            sqlite_cursor.execute(query)
        except sqlite3.Error as sqlite_err:
            raise f"SQLite error while SELECT: {sqlite_err}"

        prepared_dt_data = []
        while True:
            data: list | None = sqlite_cursor.fetchmany(100)
            if not data:
                break
            dt_data: list = [dt(*element) for element in data]
            prepared_dt_data.extend(dt_data)

        return prepared_dt_data


def load_to_postgres(data_from_sqlite, pg_conn, dt: dataclasses):
    """ Загрузка данных в Postgres. """

    column_names: list = [field.name for field in fields(data_from_sqlite[0])]
    column_names_prepared: str = ", ".join(column_names)
    col_count: str = ", ".join(["%s"] * len(column_names))

    with pg_conn.cursor() as pg_cursor:
        bind_values: str = ",".join(
            pg_cursor.mogrify(f"({col_count})", astuple(item)).decode("utf-8") for item in data_from_sqlite)

        table_name: str = dataclass_pg_tables_mapping[dt]

        column_names_for_pg: str = column_names_prepared.replace(
            "created_at", "created",
        ).replace(
            "updated_at", "modified",
        )

        query: str = (f"INSERT INTO {table_name} ({column_names_for_pg}) VALUES {bind_values} ON CONFLICT (id) DO "
                      f"NOTHING")

        try:
            pg_cursor.execute(query)
        except psycopg2.Error as pg_err:
            raise f"PostgreSQL error while INSERT: {pg_err}"


def load_data_from_sqlite_to_postgres(sqlite_conn: sqlite3.Connection, pg_conn: pg_connection):
    """ Основной метод загрузки данных из SQLite в Postgres. """

    for dt, sqlite_table in dataclass_sqlite_tables_mapping.items():

        data_from_sqlite = load_data_from_sqlite(sqlite_conn, dt, sqlite_table)

        load_to_postgres(data_from_sqlite, pg_conn, dt)


if __name__ == "__main__":
    PG_DSL = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }

    SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", "db.sqlite")

    try:
        with (sqlite_conn_context(SQLITE_DB_PATH) as sqlite_conn,
              psycopg2.connect(**PG_DSL, cursor_factory=DictCursor) as pg_conn):
            load_data_from_sqlite_to_postgres(sqlite_conn, pg_conn)
    except Exception as exc:
        raise f"Exception while loading data from sqlite to postgres: {exc}"
    finally:
        pg_conn.close()
