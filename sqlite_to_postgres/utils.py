import sqlite3
from contextlib import contextmanager


@contextmanager
def sqlite_conn_context(db_path: str):
    connection = sqlite3.connect(db_path)
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def sqlite_curs_context(sqlite_conn: sqlite3.Connection):
    sqlite_cursor = sqlite_conn.cursor()
    try:
        yield sqlite_cursor
    finally:
        sqlite_cursor.close()
