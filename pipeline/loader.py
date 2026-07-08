from abc import ABC, abstractmethod
import pandas as pd
import psycopg2.extras
from utils.db import get_connection
from utils.schema import CREATE_SCHEMAS, CREATE_BRONZE_WEATHER, CREATE_BRONZE_INDEXES


class BaseLoader(ABC):
    def __init__(self, logger=None):
        self.logger = logger

    def _log(self, level, message):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    def load(self, rows):
        if not rows:
            self._log("warning", "No rows to load — skipping.")
            return 0
        return self._write(rows)

    @abstractmethod
    def _write(self, rows):
        pass

    def __str__(self):
        return self.__class__.__name__


class PostgreSQLLoader(BaseLoader):
    # columns that map to the database table
    COLUMNS = [
        "location", "latitude", "longitude",
        "extracted_at", "processed_at",
        "temperature", "humidity", "wind_speed",
        "precipitation", "weather_code",
        "weather_description", "heat_index",
        "comfort_level", "pipeline_version",
    ]

    def __init__(self, logger=None):
        super().__init__(logger)
        self._ensure_table()            # create table if it doesn't exist

    def _ensure_table(self):

        try:
            conn = get_connection()
            cur  = conn.cursor()
            cur.execute(CREATE_SCHEMAS)
            cur.execute(CREATE_BRONZE_WEATHER)
            cur.execute(CREATE_BRONZE_INDEXES)
            conn.commit()
            cur.close()
            conn.close()
            self._log("info", "Bronze table ready (bronze_weather)")
        except Exception as e:
            self._log("error", f"Failed to create table: {e}")
            raise

    def _write(self, rows):

        df = pd.DataFrame(rows)

        cols_to_insert = [c for c in self.COLUMNS if c in df.columns]
        
        df = df[cols_to_insert]

        records = [tuple(row) for row in df.itertuples(index=False)]

        insert_sql = f"""
            INSERT INTO bronze.weather ({', '.join(cols_to_insert)})
            VALUES %s
        """

        try:
            conn = get_connection()
            cur  = conn.cursor()

            # execute_values does batch insert efficiently
            psycopg2.extras.execute_values(
                cur, insert_sql, records, page_size=100
            )

            conn.commit()
            rows_inserted = cur.rowcount
            cur.close()
            conn.close()

            self._log("success",
                f"Inserted {len(records)} row(s) into bronze_weather"
            )
            return len(records)

        except Exception as e:
            self._log("error", f"Insert failed: {e}")
            raise

    def fetch_latest(self, limit=10):
        try:
            conn = get_connection()
            cur  = conn.cursor()
            cur.execute("""
                SELECT location, extracted_at, temperature,
                       humidity, weather_description, comfort_level
                FROM   bronze.weather
                ORDER  BY created_at DESC
                LIMIT  %s
            """, (limit,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return rows
        except Exception as e:
            self._log("error", f"Fetch failed: {e}")
            return []

    def __str__(self):
        return "PostgreSQLLoader → bronze_weather"