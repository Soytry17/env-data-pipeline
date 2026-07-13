from utils.db import get_connection


def get_source(source_id):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                id,
                source_name,
                api_url,
                rate_limit,
                timeout_sec
            FROM config.sources
            WHERE id=%s
              AND is_active=TRUE
        """, (source_id,))

        row = cur.fetchone()

        if not row:
            return None

        return {
            "id": row[0],
            "source_name": row[1],
            "api_url": row[2],
            "rate_limit": row[3],
            "timeout_sec": row[4],
        }

    finally:
        cur.close()
        conn.close()