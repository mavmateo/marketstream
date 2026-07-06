
from contextlib import contextmanager
import psycopg2

@contextmanager
def get_db_cursor():
    conn   = None
    cursor = None
    try:
        conn   = psycopg2.connect(
            host="localhost", port=5432,
            dbname="marketstream",
            user="postgres", password="postgres",
        )
        cursor = conn.cursor()
        yield cursor          
        conn.commit()         
    except Exception:
        if conn: conn.rollback()
        raise
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()