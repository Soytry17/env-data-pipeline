from utils.db import test_connection, setup_schemas

print("── Database Setup ────────────────────────")

if not test_connection():
    print("Fix your .env credentials first.")
    exit()

setup_schemas()

print("\n── Verify schemas ────────────────────────")
from utils.db import get_connection

conn = get_connection()
cur  = conn.cursor()



cur.execute("""
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name IN ('bronze', 'silver', 'gold')
    ORDER BY schema_name;
""")
schemas = cur.fetchall()
print("Schemas found:")
for s in schemas:
    print(f"  ✔ {s[0]}")

cur.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    ORDER BY table_schema, table_name;
""")
tables = cur.fetchall()
print("\nTables found:")
for t in tables:
    print(f"  ✔ {t[0]}.{t[1]}")

cur.close()
conn.close()