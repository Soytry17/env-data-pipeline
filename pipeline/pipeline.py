import time

from utils.logger import Logger

class Pipeline:
    def __init__(self, extractor, validator, transformer, loader, name="ETL Pipeline"):
        self.name        = name
        self.logger      = Logger(name.replace(" ", "_"))
        self.extractor   = extractor
        self.validator   = validator
        self.transformer = transformer
        self.loader      = loader
        self._run_meta   = {}

        # inject shared logger into every stage
        self.extractor.logger   = self.logger
        self.validator.logger   = self.logger
        self.transformer.logger = self.logger
        self.loader.logger      = self.logger

    def run_pipeline(self):
        self.logger.info(f"Starting: {self.name}")
        total_start = time.time()

        # Extract
        self.logger.info("Stage 1/4 — Extract")
        t0       = time.time()
        raw_rows = self.extractor.extract()
        t_extract = round(time.time() - t0, 3)

        # Validate
        self.logger.info("Stage 2/4 — Validate")
        t0 = time.time()
        valid_rows, val_result = self.validator.validate(raw_rows)
        t_validate = round(time.time() - t0, 3)

        if not valid_rows:
            self.logger.error("All rows rejected by validator — aborting.")
            return []

        # Transform
        self.logger.info("Stage 3/4 — Transform")
        t0         = time.time()
        clean_rows = self.transformer.transform(valid_rows)
        t_transform = round(time.time() - t0, 3)

        # Load
        self.logger.info("Stage 4/4 — Load")
        t0 = time.time()
        rows_loaded = self.loader.load(clean_rows)
        t_load = round(time.time() - t0, 3)

        total_time = round(time.time() - total_start, 3)

        self._run_meta = {
            "rows_extracted":  len(raw_rows),
            "rows_rejected":   val_result.rejected_rows,
            "rows_transformed": len(clean_rows),
            "rows_loaded":     rows_loaded,
            "rejection_rate":  val_result.rejection_rate(),
            "extract_time":    t_extract,
            "validate_time":   t_validate,
            "transform_time":  t_transform,
            "load_time":       t_load,
            "total_time":      total_time,
        }

        self._print_summary()
        return clean_rows

    def _print_summary(self):
        m = self._run_meta
        self.logger.success(f"Pipeline done in {m['total_time']}s")
        print(f"\n{'═'*50}")
        print(f"  PIPELINE RUN SUMMARY")
        print(f"{'═'*50}")
        print(f"  extracted   : {m['rows_extracted']}")
        print(f"  rejected    : {m['rows_rejected']} ({m['rejection_rate']}%)")
        print(f"  transformed : {m['rows_transformed']}")
        print(f"  loaded      : {m['rows_loaded']}")
        print(f"{'─'*50}")
        print(f"  extract     : {m['extract_time']}s")
        print(f"  validate    : {m['validate_time']}s")
        print(f"  transform   : {m['transform_time']}s")
        print(f"  load        : {m['load_time']}s")
        print(f"  total       : {m['total_time']}s")
        print(f"{'═'*50}\n")

    def get_meta(self):
        return self._run_meta