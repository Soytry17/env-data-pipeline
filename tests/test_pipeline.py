from pipeline.extractor import APIExtractor
from pipeline.validator import WeatherValidator
from pipeline.loader import PostgreSQLLoader
from pipeline.pipeline import Pipeline
from utils.db import test_connection

# test DB connection first
print("Testing database connection...")
if not test_connection():
    print("Fix your .env credentials before continuing.")
    exit()

# build pipeline
pipeline = Pipeline(
    extractor   = APIExtractor(source="Open-Meteo"),
    validator   = WeatherValidator(),
    loader      = PostgreSQLLoader(),
    name        = "Weather Pipeline"
)

# run it
clean_rows = pipeline.run_pipeline()

# verify data landed in PostgreSQL
print("\nVerifying data in PostgreSQL...")
loader = PostgreSQLLoader()
latest = loader.fetch_latest(limit=5)

print(f"\n── Latest rows in bronze_weather ─────────")
for row in latest:
    print(f"  {row[0]:<15} | {str(row[1]):<20} | "
          f"temp: {row[2]}°C | humidity: {row[3]}% | "
          f"{row[4]} | {row[5]}")