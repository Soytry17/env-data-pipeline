import psycopg2
import os
from utils.schema import CREATE_SCHEMAS, CREATE_BRONZE_WEATHER, CREATE_BRONZE_INDEXES, BRONZE_COMMENTS
from dotenv import load_dotenv

load_dotenv()

def get_connection():

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )

def test_connection():
    try:
        conn = get_connection()
        cursor = conn.cursor()
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
        cur  = conn.cursor()

        print("[DB] Creating schemas: bronze, silver, gold...")
        cur.execute(CREATE_SCHEMAS)

        print("[DB] Creating bronze.weather table...")
        cur.execute(CREATE_BRONZE_WEATHER)
        cur.execute(CREATE_BRONZE_INDEXES)
        cur.execute(BRONZE_COMMENTS)

        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Schema setup complete.")

    except Exception as e:
        print(f"[DB] Schema setup failed: {e}")
        raise