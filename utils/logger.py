import os
from datetime import datetime
class Logger:

    def __init__(self, name="Pipeline", log_dir="logs"):
        self.name    = name
        self.log_dir = log_dir
        self._logs   = []                    
        self._ensure_log_dir()
        self.log_file = self._create_log_file()

    # ─── SETUP ────────────────────────────────────────────
    def _ensure_log_dir(self):
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

    def _create_log_file(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = f"{self.log_dir}/{self.name}_{timestamp}.log"
        return filename

    # ─── CORE LOG METHOD ──────────────────────────────────
    def _log(self, level, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        entry = f"[{timestamp}] [{level}] [{self.name}] {message}"

        # Print to console
        print(entry)

        # Write to file
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(entry + "\n")

        # Store in memory
        self._logs.append({
            "timestamp": timestamp,
            "level": level,
            "message": message
        })

    # ─── PUBLIC METHODS ───────────────────────────────────
    def info(self, message):
        self._log("INFO", message)

    def warning(self, message):
        self._log("WARNING", message)

    def error(self, message):
        self._log("ERROR", message)

    def success(self, message):
        self._log("SUCCESS", message)

    # ─── SUMMARY ──────────────────────────────────────────
    def get_logs(self):
        return self._logs

    def summary(self):
        total    = len(self._logs)
        warnings = sum(1 for l in self._logs if l["level"] == "WARNING")
        errors   = sum(1 for l in self._logs if l["level"] == "ERROR")
        print(f"\n{'─'*50}")
        print(f"  LOG SUMMARY — {self.name}")
        print(f"{'─'*50}")
        print(f"  total entries  : {total}")
        print(f"  warnings       : {warnings}")
        print(f"  errors         : {errors}")
        print(f"  log file       : {self.log_file}")
        print(f"{'─'*50}\n")

    def __str__(self):
        return f"Logger(name={self.name}, file={self.log_file})"