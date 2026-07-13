import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db import get_connection


# ─── SQL DEFINITIONS

CREATE_CONFIG_SCHEMA = "CREATE SCHEMA IF NOT EXISTS config;"

CREATE_SOURCES_TABLE = """
CREATE TABLE IF NOT EXISTS config.sources (
    id            SERIAL PRIMARY KEY,
    source_name   VARCHAR(100)  NOT NULL,
    api_url       TEXT          NOT NULL,
    frequency     VARCHAR(20)   NOT NULL,
    description   TEXT,
    rate_limit    INTEGER,
    requires_key  BOOLEAN       DEFAULT FALSE,
    timeout_sec   INTEGER       DEFAULT 30,
    is_active     BOOLEAN       DEFAULT TRUE,
    created_at    TIMESTAMP     DEFAULT NOW()
);
"""

CREATE_LOCATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS config.locations (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(100)  NOT NULL,
    name_khmer    VARCHAR(100),
    latitude      NUMERIC(9, 6) NOT NULL,
    longitude     NUMERIC(9, 6) NOT NULL,
    region        VARCHAR(50),
    population    INTEGER,
    elevation_m   INTEGER,
    timezone      VARCHAR(50)   DEFAULT 'Asia/Phnom_Penh',
    is_active     BOOLEAN       DEFAULT TRUE,
    created_at    TIMESTAMP     DEFAULT NOW()
);
"""

CREATE_SOURCE_LOCATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS config.source_locations (
    id             SERIAL PRIMARY KEY,
    source_id      INTEGER NOT NULL REFERENCES config.sources(id),
    location_id    INTEGER NOT NULL REFERENCES config.locations(id),
    custom_params  JSONB,
    priority       INTEGER   DEFAULT 1,
    last_fetched   TIMESTAMP,
    is_active      BOOLEAN   DEFAULT TRUE,
    created_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (source_id, location_id)
);
"""

# ─── SEED DATA

SOURCES = [
    {
        "source_name":  "Open-Meteo",
        "api_url":      "https://api.open-meteo.com/v1/forecast",
        "frequency":    "hourly",
        "description":  "Free weather API — temperature, humidity, wind, precipitation",
        "rate_limit":   10000,
        "requires_key": False,
        "timeout_sec":  30,
    }
]

# all 25 cambodian provinces
LOCATIONS = [
    # ── Central ───────────────────────────────────────────
    {
        "name":        "Phnom Penh",
        "name_khmer":  "ភ្នំពេញ",
        "latitude":    11.5564,
        "longitude":   104.9282,
        "region":      "Central",
        "population":  2281951,
        "elevation_m": 12,
    },
    {
        "name":        "Kandal",
        "name_khmer":  "កណ្តាល",
        "latitude":    11.2232,
        "longitude":   105.1256,
        "region":      "Central",
        "population":  1265280,
        "elevation_m": 10,
    },
    {
        "name":        "Kampong Speu",
        "name_khmer":  "កំពង់ស្ពឺ",
        "latitude":    11.4535,
        "longitude":   104.5196,
        "region":      "Central",
        "population":  952744,
        "elevation_m": 30,
    },
    {
        "name":        "Takeo",
        "name_khmer":  "តាកែវ",
        "latitude":    10.9896,
        "longitude":   104.7998,
        "region":      "Central",
        "population":  950000,
        "elevation_m": 8,
    },
    {
        "name":        "Kampot",
        "name_khmer":  "កំពត",
        "latitude":    10.5936,
        "longitude":   104.1638,
        "region":      "Central",
        "population":  628700,
        "elevation_m": 5,
    },

    # ── Northern ──────────────────────────────────────────
    {
        "name":        "Siem Reap",
        "name_khmer":  "សៀមរាប",
        "latitude":    13.3671,
        "longitude":   103.8448,
        "region":      "Northern",
        "population":  1234000,
        "elevation_m": 20,
    },
    {
        "name":        "Kampong Thom",
        "name_khmer":  "កំពង់ធំ",
        "latitude":    12.7111,
        "longitude":   104.8897,
        "region":      "Northern",
        "population":  708040,
        "elevation_m": 25,
    },
    {
        "name":        "Kampong Cham",
        "name_khmer":  "កំពង់ចាម",
        "latitude":    11.9933,
        "longitude":   105.4637,
        "region":      "Northern",
        "population":  1679992,
        "elevation_m": 18,
    },
    {
        "name":        "Kratie",
        "name_khmer":  "ក្រចេះ",
        "latitude":    12.4882,
        "longitude":   106.0188,
        "region":      "Northern",
        "population":  386954,
        "elevation_m": 55,
    },
    {
        "name":        "Stung Treng",
        "name_khmer":  "ស្ទឹងត្រែង",
        "latitude":    13.5228,
        "longitude":   105.9699,
        "region":      "Northern",
        "population":  179912,
        "elevation_m": 68,
    },
    {
        "name":        "Preah Vihear",
        "name_khmer":  "ព្រះវិហារ",
        "latitude":    13.7952,
        "longitude":   104.9719,
        "region":      "Northern",
        "population":  225754,
        "elevation_m": 180,
    },
    {
        "name":        "Oddar Meanchey",
        "name_khmer":  "អូដ្ឋរមានជ័យ",
        "latitude":    14.1804,
        "longitude":   103.5166,
        "region":      "Northern",
        "population":  262387,
        "elevation_m": 120,
    },
    {
        "name":        "Banteay Meanchey",
        "name_khmer":  "បន្ទាយមានជ័យ",
        "latitude":    13.7527,
        "longitude":   102.9893,
        "region":      "Northern",
        "population":  768648,
        "elevation_m": 35,
    },
    {
        "name":        "Pailin",
        "name_khmer":  "បៃលិន",
        "latitude":    12.8496,
        "longitude":   102.6093,
        "region":      "Northern",
        "population":  70482,
        "elevation_m": 150,
    },
    {
        "name":        "Battambang",
        "name_khmer":  "បាត់ដំបង",
        "latitude":    13.0957,
        "longitude":   103.2022,
        "region":      "Northern",
        "population":  1136242,
        "elevation_m": 28,
    },
    {
        "name":        "Pursat",
        "name_khmer":  "បុរីសត្វ",
        "latitude":    12.5388,
        "longitude":   103.9194,
        "region":      "Northern",
        "population":  466578,
        "elevation_m": 22,
    },

    # ── Highland
    {
        "name":        "Mondulkiri",
        "name_khmer":  "មណ្ឌលគីរី",
        "latitude":    12.4537,
        "longitude":   107.1885,
        "region":      "Highland",
        "population":  68279,
        "elevation_m": 750,
    },
    {
        "name":        "Ratanakiri",
        "name_khmer":  "រតនគីរី",
        "latitude":    13.7299,
        "longitude":   107.0098,
        "region":      "Highland",
        "population":  194673,
        "elevation_m": 620,
    },

    # ── Coastal
    {
        "name":        "Sihanoukville",
        "name_khmer":  "ព្រះសីហនុ",
        "latitude":    10.6333,
        "longitude":   103.5000,
        "region":      "Coastal",
        "population":  298379,
        "elevation_m": 5,
    },
    {
        "name":        "Koh Kong",
        "name_khmer":  "កោះកុង",
        "latitude":    11.6152,
        "longitude":   102.9846,
        "region":      "Coastal",
        "population":  139722,
        "elevation_m": 10,
    },
    {
        "name":        "Kep",
        "name_khmer":  "កែប",
        "latitude":    10.4833,
        "longitude":   104.3167,
        "region":      "Coastal",
        "population":  47600,
        "elevation_m": 5,
    },

    # ── Eastern
    {
        "name":        "Svay Rieng",
        "name_khmer":  "ស្វាយរៀង",
        "latitude":    11.0875,
        "longitude":   105.7996,
        "region":      "Eastern",
        "population":  526176,
        "elevation_m": 12,
    },
    {
        "name":        "Prey Veng",
        "name_khmer":  "ព្រៃវែង",
        "latitude":    11.4851,
        "longitude":   105.3252,
        "region":      "Eastern",
        "population":  1438990,
        "elevation_m": 10,
    },
    {
        "name":        "Tboung Khmum",
        "name_khmer":  "ត្បូងឃ្មុំ",
        "latitude":    11.9000,
        "longitude":   105.6667,
        "region":      "Eastern",
        "population":  750000,
        "elevation_m": 20,
    },
    {
        "name":        "Kampong Chhnang",
        "name_khmer":  "កំពង់ឆ្នាំង",
        "latitude":    12.2500,
        "longitude":   104.6667,
        "region":      "Eastern",
        "population":  554132,
        "elevation_m": 15,
    },
]


def setup_config():
    conn = get_connection()
    cur  = conn.cursor()

    try:
        # ── create schema and tables
        print("── Creating config schema...")
        cur.execute(CREATE_CONFIG_SCHEMA)
        cur.execute(CREATE_SOURCES_TABLE)
        cur.execute(CREATE_LOCATIONS_TABLE)
        cur.execute(CREATE_SOURCE_LOCATIONS_TABLE)
        conn.commit()
        print("   ✔ Tables created")

        # ── seed sources
        print("\n── Seeding config.sources...")
        for source in SOURCES:
            cur.execute("""
                INSERT INTO config.sources
                    (source_name, api_url, frequency, description,
                     rate_limit, requires_key, timeout_sec)
                VALUES
                    (%(source_name)s, %(api_url)s, %(frequency)s,
                     %(description)s, %(rate_limit)s, %(requires_key)s,
                     %(timeout_sec)s)
                ON CONFLICT DO NOTHING
            """, source)
        conn.commit()
        cur.execute("SELECT id, source_name FROM config.sources")
        sources = cur.fetchall()
        print(f"   ✔ {len(sources)} source(s) seeded")
        for s in sources:
            print(f"      id={s[0]} → {s[1]}")

        # ── seed locations
        print("\n── Seeding config.locations (25 provinces)...")
        for loc in LOCATIONS:
            cur.execute("""
                INSERT INTO config.locations
                    (name, name_khmer, latitude, longitude,
                     region, population, elevation_m)
                VALUES
                    (%(name)s, %(name_khmer)s, %(latitude)s, %(longitude)s,
                     %(region)s, %(population)s, %(elevation_m)s)
                ON CONFLICT DO NOTHING
            """, loc)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM config.locations")
        count = cur.fetchone()[0]
        print(f"   ✔ {count} province(s) seeded")

        # ── seed source_locations (Open-Meteo × all 25 provinces)
        print("\n── Seeding config.source_locations...")
        cur.execute("SELECT id FROM config.sources WHERE source_name = 'Open-Meteo'")
        source_id = cur.fetchone()[0]

        cur.execute("SELECT id FROM config.locations")
        location_ids = [row[0] for row in cur.fetchall()]

        for loc_id in location_ids:
            cur.execute("""
                INSERT INTO config.source_locations
                    (source_id, location_id, priority)
                VALUES (%s, %s, 1)
                ON CONFLICT (source_id, location_id) DO NOTHING
            """, (source_id, loc_id))

        conn.commit()
        cur.execute("SELECT COUNT(*) FROM config.source_locations")
        sl_count = cur.fetchone()[0]
        print(f"   ✔ {sl_count} source-location pair(s) seeded")

        # ── final verification
        print("\n── Verification query:")
        cur.execute("""
            SELECT
                s.source_name,
                l.name,
                l.name_khmer,
                l.region,
                l.elevation_m
            FROM config.source_locations sl
            JOIN config.sources   s ON s.id = sl.source_id
            JOIN config.locations l ON l.id = sl.location_id
            WHERE sl.is_active = TRUE
            ORDER BY l.region, l.name
        """)
        rows = cur.fetchall()
        print(f"\n{'─'*65}")
        print(f"  {'Source':<12} {'Province':<22} {'Khmer':<18} {'Region':<10} {'Elev'}")
        print(f"{'─'*65}")
        for row in rows:
            print(f"  {row[0]:<12} {row[1]:<22} {row[2]:<18} {row[3]:<10} {row[4]}m")
        print(f"{'─'*65}")
        print(f"  Total: {len(rows)} active source-location pairs\n")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    setup_config()