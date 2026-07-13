import os

import psycopg2
from dotenv import load_dotenv

from utils.schema import CREATE_SCHEMAS

load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )


def test_connection():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT current_user;")
        print("Current user:", cursor.fetchone())

        cursor.execute("SELECT current_database();")
        print("Database:", cursor.fetchone())
        cursor.execute("SELECT VERSION();")
        db_version = cursor.fetchone()
        print(f'Database version: {db_version}')
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f'Database error: {e}')
        return False


def setup_schemas():
    try:
        conn = get_connection()
        cur = conn.cursor()

        print("[DB] Creating schemas: bronze, silver, gold...")
        cur.execute(CREATE_SCHEMAS)

        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Schema setup complete.")

    except Exception as e:
        print(f"[DB] Schema setup failed: {e}")
        raise
