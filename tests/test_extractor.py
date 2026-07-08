from pipeline.extractor import APIExtractor
from pipeline.validator import WeatherValidator
from pipeline.transformer import WeatherTransformer
from utils.logger import Logger

logger      = Logger("PipelineTest")
extractor   = APIExtractor(source="Open-Meteo", logger=logger)
validator   = WeatherValidator(logger=logger)
transformer = WeatherTransformer(logger=logger)

raw_rows = extractor.extract()

valid_rows, result = validator.validate(raw_rows)

if not result.passed():
    print(f"\nWarning: {result.rejected_rows} row(s) rejected")
    print(f"Rejection rate: {result.rejection_rate()}%")

# Stage 3 — transform (only valid rows)
clean_rows = transformer.transform(valid_rows)

print(f"\nFinal: {len(clean_rows)} clean row(s) ready to load")