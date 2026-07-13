# pipeline/validator.py

from dataclasses import dataclass, field
from typing import List
from datetime import datetime, timezone, timedelta


@dataclass
class ValidationResult:
    total_rows: int = 0
    passed_rows: int = 0
    rejected_rows: int = 0
    rejections: List[dict] = field(default_factory=list)

    def passed(self):
        return self.rejected_rows == 0

    def rejection_rate(self):
        if self.total_rows == 0:
            return 0.0
        return round(self.rejected_rows / self.total_rows * 100, 1)


class WeatherValidator:
    """
    Responsibility:
    - Did the API return a valid response?
    - Is the JSON structure what we expect?
    - Are required fields present?
    - Is the HTTP response clean?

    NOT responsible for:
    - Value ranges (dbt handles this)
    - Null handling (dbt handles this)
    - Duplicate detection (dbt handles this)
    - Business rules (dbt handles this)
    """

    # fields that must exist inside raw_data["current"]
    REQUIRED_CURRENT_FIELDS = [
        "time",
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "precipitation",
        "weather_code",
    ]

    # top-level fields that must exist in the raw API response
    REQUIRED_TOP_LEVEL_FIELDS = [
        "latitude",
        "longitude",
        "timezone",
        "current",
    ]

    def __init__(self, logger=None):
        self.logger = logger
        self.result = ValidationResult()

    def _log(self, level, message):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    # ─── MAIN ENTRY POINT

    def validate(self, rows):
        """
        Validates a list of raw extracted rows.
        Each row contains:
            source_id, location_id, location_name,
            observation_at, raw_data (full API response)

        Returns (valid_rows, ValidationResult)
        """
        self.result = ValidationResult(total_rows=len(rows))

        valid_rows = []
        rejected_rows = []

        for row in rows:
            passed, reason = self._validate_row(row)
            if passed:
                valid_rows.append(row)
            else:
                rejected_rows.append({**row, "rejection_reason": reason})
                self._log("warning",
                          f"  [rejected] {row.get('location_name', '?')} "
                          f"→ {reason}"
                          )

        self.result.passed_rows = len(valid_rows)
        self.result.rejected_rows = len(rejected_rows)
        self.result.rejections = rejected_rows

        self._print_summary()
        return valid_rows, self.result

    # ─── ROW LEVEL VALIDATION

    def _validate_row(self, row):

        # rule 1 — required pipeline fields must exist
        passed, reason = self._rule_required_pipeline_fields(row)
        if not passed:
            return False, reason

        # rule 2 — raw_data must be a dict
        passed, reason = self._rule_raw_data_is_dict(row)
        if not passed:
            return False, reason

        # rule 3 — no API error in response
        passed, reason = self._rule_no_api_error(row)
        if not passed:
            return False, reason

        # rule 4 — required top-level fields exist in raw_data
        passed, reason = self._rule_top_level_fields(row)
        if not passed:
            return False, reason

        # rule 5 — current block exists and is a dict
        passed, reason = self._rule_current_block_exists(row)
        if not passed:
            return False, reason

        # rule 6 — required fields exist inside current block
        passed, reason = self._rule_required_current_fields(row)
        if not passed:
            return False, reason

        # rule 7 — observation_at is present and not in future
        passed, reason = self._rule_valid_observation_at(row)
        if not passed:
            return False, reason

        # rule 8 — source_id and location_id are valid integers
        passed, reason = self._rule_valid_ids(row)
        if not passed:
            return False, reason

        return True, None

    # ─── RULES ────────────────────────────────────────────

    def _rule_required_pipeline_fields(self, row):

        # Row must have source_id, location_id, raw_data

        required = ["source_id", "location_id", "raw_data"]

        missing = [f for f in required if f not in row or row[f] is None]

        if missing:
            return False, f"Missing pipeline fields: {missing}"

        return True, None

    def _rule_raw_data_is_dict(self, row):
        # raw_data must be a dictionary
        if not isinstance(row.get("raw_data"), dict):
            return False, f"raw_data is not a dict — got {type(row.get('raw_data')).__name__}"
        return True, None

    def _rule_no_api_error(self, row):
        # I response must not contain an error field
        raw = row.get("raw_data", {})
        if "error" in raw:
            return False, f"API returned error: {raw.get('reason', raw.get('error'))}"
        if "reason" in raw and "error" in str(raw.get("reason", "")).lower():
            return False, f"API error reason: {raw['reason']}"
        return True, None

    def _rule_top_level_fields(self, row):
        # Required top-level fields must exist in raw_data
        raw = row.get("raw_data", {})
        missing = [f for f in self.REQUIRED_TOP_LEVEL_FIELDS if f not in raw]
        if missing:
            return False, f"Missing top-level fields in raw_data: {missing}"
        return True, None

    def _rule_current_block_exists(self, row):
        # raw_data['current'] must be a non-empty dict
        current = row.get("raw_data", {}).get("current")
        if not current:
            return False, "raw_data['current'] is missing or empty"
        if not isinstance(current, dict):
            return False, f"raw_data['current'] is not a dict — got {type(current).__name__}"
        return True, None

    def _rule_required_current_fields(self, row):
        # All required measurement fields must exist in current block
        current = row.get("raw_data", {}).get("current", {})
        missing = [
            f for f in self.REQUIRED_CURRENT_FIELDS
            if f not in current
        ]
        if missing:
            return False, f"Missing current fields: {missing}"
        return True, None

    def _rule_valid_observation_at(self, row):
        # observation_at must be present and not in the future.
        obs_at = row.get("observation_at")
        if not obs_at:
            return False, "observation_at is missing or empty"

        try:
            parsed = datetime.fromisoformat(str(obs_at))
        except ValueError:
            return False, f"observation_at is not a valid datetime: {obs_at}"

        offset_seconds = row.get("raw_data", {}).get("utc_offset_seconds", 0) or 0

        if parsed.tzinfo is None:
            # naive local wall-clock time → shift to UTC
            parsed_utc = (
                parsed - timedelta(seconds=offset_seconds)
            ).replace(tzinfo=timezone.utc)
        else:
            parsed_utc = parsed.astimezone(timezone.utc)

        now_utc = datetime.now(timezone.utc)

        # small grace window for clock skew and reporting-interval rounding
        if parsed_utc > now_utc + timedelta(minutes=5):
            return False, f"observation_at is in the future: {obs_at}"

        return True, None

    def _rule_valid_ids(self, row):

        # source_id and location_id must be positive integers

        for field_name in ["source_id", "location_id"]:
            val = row.get(field_name)
            if not isinstance(val, int) or val <= 0:
                return False, f"{field_name} must be a positive integer — got {val}"
        return True, None

    # ─── SUMMARY

    def _print_summary(self):
        r = self.result
        self._log("info",
                  f"Validation complete — "
                  f"total: {r.total_rows} | "
                  f"passed: {r.passed_rows} | "
                  f"rejected: {r.rejected_rows} | "
                  f"rejection rate: {r.rejection_rate()}%"
                  )
        if r.rejections:
            self._log("warning", f"Rejected rows saved for review:")
            for row in r.rejections:
                self._log("warning",
                          f"  {row.get('location_name', '?'):<22} "
                          f"→ {row.get('rejection_reason')}"
                          )
