import os
import sys
from datetime import datetime

from utils.schema import GRANT_BRONZE, CREATE_BRONZE_WEATHER, CREATE_INDEXES, ADD_COMMENTS, \
    get_or_create_partition

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db import get_connection

def setup_bronze():
    conn = get_connection()
    cur = conn.cursor()

    try:
        # ── create schema
        print("── Grant bronze schema...")
        cur.execute(GRANT_BRONZE)
        conn.commit()
        print("   ✔ Schema Granted")

        # ── create parent table
        print("\n── Creating bronze.weather (partitioned parent table)...")
        cur.execute(CREATE_BRONZE_WEATHER)
        conn.commit()
        print("   ✔ Parent table created")

        # ── create yearly partitions 2000 → 2026
        print("\n── Creating yearly partitions (2000 → 2026)...")
        partitions_created = []

        current_year = datetime.now().year

        for year in range(2020, current_year + 1):
            table_name = create_yearly_partition(cur, year)
            partitions_created.append((year, table_name))
            print(f"   ✔ {table_name}")

        conn.commit()
        print(f"\n   Total: {len(partitions_created)} partitions created")

        # ── create indexes
        print("\n── Creating indexes...")
        cur.execute(CREATE_INDEXES)
        conn.commit()
        print("   ✔ Indexes created")

        # ── add comments
        print("\n── Adding column comments...")
        cur.execute(ADD_COMMENTS)
        conn.commit()
        print("   ✔ Comments added")

        # ── verification
        print("\n── Verification:")
        cur.execute("""
            SELECT
                nmsp_parent.nspname  AS schema,
                parent.relname       AS parent_table,
                child.relname        AS partition,
                pg_get_expr(
                    child.relpartbound,
                    child.oid
                )                    AS partition_range
            FROM pg_inherits
            JOIN pg_class  parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class  child  ON pg_inherits.inhrelid  = child.oid
            JOIN pg_namespace nmsp_parent
                ON nmsp_parent.oid = parent.relnamespace
            WHERE parent.relname = 'weather'
              AND nmsp_parent.nspname = 'bronze'
            ORDER BY child.relname;
        """)
        partitions = cur.fetchall()

        print(f"\n{'─' * 65}")
        print(f"  {'Schema':<10} {'Parent':<16} {'Partition':<24} Range")
        print(f"{'─' * 65}")
        for row in partitions[:5]:
            print(f"  {row[0]:<10} {row[1]:<16} {row[2]:<24} {row[3]}")
        print(f"  ... ({len(partitions)} total partitions)")
        print(f"{'─' * 65}")

        # ── check indexes
        print("\n── Indexes on bronze.weather:")
        cur.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'bronze'
              AND tablename  = 'weather'
            ORDER BY indexname;
        """)
        indexes = cur.fetchall()
        for idx in indexes:
            print(f"   ✔ {idx[0]}")

        print("\n── bronze.weather is ready!\n")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        cur.close()
        conn.close()


def add_future_partition(year):
    """
    Call this every January to add the next year partition.
    Example: add_future_partition(2027)
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        table_name = get_or_create_partition(cur, year)
        conn.commit()
        print(f"✔ Added partition: {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    setup_bronze()
