import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List


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
            return 0
        return round(self.rejected_rows / self.total_rows * 100, 2)


class WeatherValidator:
    # define schema we need

    REQUIRED_FIELDS = [
        "location", "extracted_at",
        "temperature", "humidity",
        "wind_speed", "precipitation"
    ]

    # acceptable value ranges
    RANGES = {
        "temperature": (-90, 60),
        "humidity": (0, 100),
        "wind_speed": (0, 200),
        "precipitation": (0, 500),
    }

    MAX_AGE_MINUTES = 100

    def __init__(self, logger=None):
        self.logger = logger
        self.result = ValidationResult()

    def _log(self, level, message):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(message)

    # define rule for validaion

    def _rule_required_fields(self, df, mask):

        missing_cols = [f for f in self.REQUIRED_FIELDS if f not in df.columns]

        if missing_cols:
            self._log("error", f"Missing required columns: {missing_cols}")

            return pd.Series([False] * len(df), index=df.index)

        return mask

    def _rule_no_empty_location(self, df, mask):

        empty_cols = df["location"].isna() | (df["location"].astype(str).str.strip() == "")

        if empty_cols.any():
            self._reject(df, empty_cols, "Empty location")
            mask = mask & ~empty_cols

        return mask

    def _rule_numeric_types(self, df, mask):

        numeric_cols = ["temperature", "humidity", "wind_speed", "precipitation"]

        for col in numeric_cols:
            if col not in df.columns:
                continue
            coerecd = pd.to_numeric(df[col], errors="coerce")
            bad = coerecd.isna() & df[col].notna()
            if bad.any():
                self._reject(df, bad, f"Non-numeric value in '{col}'")
                mask = mask & ~bad
        return mask

    def _rule_value_ranges(self, df, mask):

        for col, (low, high) in self.RANGES.items():
            if col not in df.columns:
                continue
            values = pd.to_numeric(df[col], errors="coerce")
            bad = values.notna() & ((values < low) | (values > high))
            if bad.any():
                self._reject(df, bad, f"'{col}' out of range [{low}, {high}]")
                mask = mask & ~bad
        return mask

    def _rule_no_future_timestamps(self, df, mask):

        if "extracted_at" not in df.columns:
            return mask
        timestamps = pd.to_datetime(df["extracted_at"], errors="coerce")
        now = pd.Timestamp.now()
        bad = timestamps > now
        if bad.any():
            self._reject(df, bad, "Future timestamp in 'extracted_at'")
            mask = mask & ~bad
        return mask

    def _rule_no_duplicate_location(self, df, mask):
        """Each location should appear only once per extraction"""
        duplicated = df.duplicated(subset=["location"], keep="first")
        if duplicated.any():
            self._reject(df, duplicated, "Duplicate location in same batch")
            mask = mask & ~duplicated
        return mask

    def _print_summary(self):
        r = self.result
        self._log("info", (
            f"Validation complete — "
            f"total: {r.total_rows} | "
            f"passed: {r.passed_rows} | "
            f"rejected: {r.rejected_rows} | "
            f"rejection rate: {r.rejection_rate()}%"
        ))
        if r.rejections:
            self._log("warning", "Rejected rows:")
            for row in r.rejections:
                self._log("warning",
                          f"  {row.get('location', '?')} → {row.get('rejection_reason')}"
                          )

    def _reject(self, df, bad_mask, reason):
        rejected = df[bad_mask].copy()
        rejected["rejection_reason"] = reason
        self.result.rejections.extend(rejected.to_dict(orient="records"))
        self._log("warning", f"Rejected {bad_mask.sum()} row(s) — {reason}")

    def validate(self, rows):

        self.result = ValidationResult(total_rows=len(rows))

        df = pd.DataFrame(rows)

        valid_mask = pd.Series([True] * len(df), index=df.index)

        valid_mask = self._rule_required_fields(df, valid_mask)
        valid_mask = self._rule_no_empty_location(df, valid_mask)
        valid_mask = self._rule_numeric_types(df, valid_mask)
        valid_mask = self._rule_value_ranges(df, valid_mask)
        valid_mask = self._rule_no_future_timestamps(df, valid_mask)
        valid_mask = self._rule_no_duplicate_location(df, valid_mask)

        # split into valid and rejected
        valid_df = df[valid_mask].reset_index(drop=True)
        rejected_df = df[~valid_mask].reset_index(drop=True)

        self.result.passed_rows = len(valid_df)
        self.result.rejected_rows = len(rejected_df)

        self._print_summary()

        # return as list of dicts — same contract as extractor
        return valid_df.to_dict(orient="records"), self.result
