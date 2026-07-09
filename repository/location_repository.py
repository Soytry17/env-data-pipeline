from utils.db import get_connection


def get_active_locations(source_id):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                l.id,
                l.name,
                l.name_khmer,
                l.latitude,
                l.longitude,
                l.region,
                l.elevation_m,
                l.timezone,
                sl.custom_params,
                sl.priority
            FROM config.source_locations sl
            JOIN config.locations l
                ON l.id = sl.location_id
            WHERE sl.source_id=%s
              AND sl.is_active=TRUE
              AND l.is_active=TRUE
            ORDER BY sl.priority DESC,l.name
        """, (source_id,))

        rows = []

        for row in cur.fetchall():
            rows.append({
                "location_id": row[0],
                "name": row[1],
                "name_khmer": row[2],
                "latitude": float(row[3]),
                "longitude": float(row[4]),
                "region": row[5],
                "elevation_m": row[6],
                "timezone": row[7],
                "custom_params": row[8] or {},
                "priority": row[9],
            })

        return rows

    finally:
        cur.close()
        conn.close()