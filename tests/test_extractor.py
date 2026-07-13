import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.extractor import APIExtractor
from pipeline.validator import WeatherValidator
from pipeline.loader    import PostgreSQLLoader
from utils.logger       import Logger

logger    = Logger("LoaderTest")
extractor = APIExtractor(source_id=1, logger=logger)
validator = WeatherValidator(logger=logger)
loader    = PostgreSQLLoader(logger=logger)

# ── Stage 1: Extract
print("\n── Stage 1: Extract ──────────────────────────")
rows = extractor.extract()

# ── Stage 2: Validate
print("\n── Stage 2: Validate ─────────────────────────")
valid_rows, result = validator.validate(rows)
print(f"  passed: {result.passed_rows} / {result.total_rows}")

# ── Stage 3: Load
print("\n── Stage 3: Load ─────────────────────────────")
inserted = loader.load(valid_rows)
print(f"  inserted: {inserted} rows")

# ── Verify in database
print("\n── Verify in bronze.weather ──────────────────")
latest = loader.fetch_latest(limit=5)
print(f"\n{'─'*80}")
print(f"  {'Source':<12} {'Location':<22} {'Region':<10} {'Temp':<8} {'Humidity'}")
print(f"{'─'*80}")
for row in latest:
    print(
        f"  {row[1]:<12} {row[2]:<22} {row[3]:<10} "
        f"{row[6]:<8} {row[7]}%"
    )
print(f"{'─'*80}")