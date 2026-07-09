from abc import ABC, abstractmethod
import json
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db import get_connection
from utils.schema import get_or_create_partition

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

    INSERT_SQL = """
        INSERT INTO bronze.weather (
            source_id,
            location_id,
            observation_at,
            raw_data,
            ingested_at
        ) VALUES (
            %(source_id)s,
            %(location_id)s,
            %(observation_at)s,
            %(raw_data)s,
            NOW()
        )
    """

    def __init__(self, logger=None):
        super().__init__(logger)
        self._valid_source_ids   = set()
        self._valid_location_ids = set()
        self._load_valid_ids()

    # ─── LOAD VALID IDS

    def _load_valid_ids(self):

        conn = get_connection()
        cur  = conn.cursor()
        try:
            cur.execute("SELECT id FROM config.sources WHERE is_active = TRUE")
            self._valid_source_ids = {row[0] for row in cur.fetchall()}

            cur.execute("SELECT id FROM config.locations WHERE is_active = TRUE")
            self._valid_location_ids = {row[0] for row in cur.fetchall()}

            self._log("info",
                f"Loaded {len(self._valid_source_ids)} source(s) and "
                f"{len(self._valid_location_ids)} location(s) for FK validation"
            )
        finally:
            cur.close()
            conn.close()

    # ─── APPLICATION LEVEL FK VALIDATION

    def _validate_ids(self, row):
        """
        Replaces database FK constraint.
        Raises ValueError if IDs are not in config tables.
        """
        if row.get("source_id") not in self._valid_source_ids:
            raise ValueError(
                f"Invalid source_id: {row.get('source_id')} "
                f"— not found in config.sources"
            )
        if row.get("location_id") not in self._valid_location_ids:
            raise ValueError(
                f"Invalid location_id: {row.get('location_id')} "
                f"— not found in config.locations"
            )

    # ─── WRITE

    def _write(self, rows):
        conn = get_connection()
        cur  = conn.cursor()

        inserted  = 0
        failed    = 0

        try:
            # ensure partitions exist for all years in this batch
            self._ensure_partitions(cur, rows)
            conn.commit()

            for row in rows:
                try:
                    # application-level FK check
                    self._validate_ids(row)

                    # prepare row for insert
                    record = {
                        "source_id":      row["source_id"],
                        "location_id":    row["location_id"],
                        "observation_at": row["observation_at"],
                        "raw_data":       json.dumps(row["raw_data"]),
                    }

                    cur.execute(self.INSERT_SQL, record)
                    inserted += 1

                    self._log("info",
                        f"  ✔ inserted '{row.get('location_name', row['location_id'])}' "
                        f"→ observation_at: {row['observation_at']}"
                    )

                except ValueError as e:
                    self._log("warning", f"  [skip] {e}")
                    failed += 1

                except Exception as e:
                    self._log("error",
                        f"  [error] Failed to insert "
                        f"'{row.get('location_name', '?')}': {e}"
                    )
                    failed += 1

            conn.commit()

            self._log("success",
                f"Load complete — "
                f"inserted: {inserted} | "
                f"failed: {failed} | "
                f"total: {len(rows)}"
            )
            return inserted

        except Exception as e:
            conn.rollback()
            self._log("error", f"Load failed — rolling back: {e}")
            raise

        finally:
            cur.close()
            conn.close()

    # ─── PARTITION MANAGEMENT

    def _ensure_partitions(self, cur, rows):
        years_needed = set()

        for row in rows:
            obs_at = row.get("observation_at")
            if obs_at:
                try:
                    year = int(str(obs_at)[:4])
                    years_needed.add(year)
                except (ValueError, TypeError):
                    pass

        for year in sorted(years_needed):
            partition = get_or_create_partition(cur, year)
            self._log("info", f"  partition ready: {partition}")

    # ─── FETCH LATEST

    def fetch_latest(self, limit=10):
        conn = get_connection()
        cur  = conn.cursor()
        try:
            cur.execute("""
                SELECT
                    b.id,
                    s.source_name,
                    l.name              AS location,
                    l.region,
                    b.observation_at,
                    b.ingested_at,
                    b.raw_data->'current'->>'temperature_2m'  AS temperature,
                    b.raw_data->'current'->>'relative_humidity_2m' AS humidity,
                    b.raw_data->'current'->>'weather_code'    AS weather_code
                FROM bronze.weather b
                JOIN config.sources   s ON s.id = b.source_id
                JOIN config.locations l ON l.id = b.location_id
                ORDER BY b.ingested_at DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
        finally:
            cur.close()
            conn.close()

    def __str__(self):
        return "PostgreSQLLoader → bronze.weather"