from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd
import numpy as np


class BaseTransformer(ABC):
    def __init__(self, logger=None):
        self.logger = logger
        self._summary = {
            "total_in":  0,
            "dropped":   0,
            "total_out": 0,
        }

    def _log(self, level, message):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    @abstractmethod
    def transform(self, rows):
        pass

    def get_summary(self):
        return self._summary

    def __str__(self):
        return self.__class__.__name__


class WeatherTransformer(BaseTransformer):

    # valid ranges for each measurement
    VALID_RANGES = {
        "temperature":   (-20, 60),
        "humidity":      (0, 100),
        "wind_speed":    (0, 200),
        "precipitation": (0, 500),
    }

    def transform(self, rows):
        self._summary["total_in"] = len(rows)

        df = pd.DataFrame(rows)
        self._log("info", f"Loaded {len(df)} rows into DataFrame")

        df = self._cast_types(df)
        df = self._parse_timestamps(df)
        df = self._fill_missing(df)
        df = self._add_weather_description(df)
        df = self._add_heat_index(df)
        df = self._add_pipeline_metadata(df)
        df = self._reorder_columns(df)

        self._summary["total_out"] = len(df)
        self._summary["dropped"]   = self._summary["total_in"] - len(df)
        self._log_summary(df)

        # return as list of dicts — same contract as old pipeline
        return df.to_dict(orient="records")

    def _cast_types(self, df):
        numeric_cols = ["temperature", "humidity", "wind_speed",
                        "precipitation", "weather_code",
                        "latitude", "longitude"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        self._log("info", "Cast all numeric columns")
        return df

    def _parse_timestamps(self, df):
        df["extracted_at"] = pd.to_datetime(
            df["extracted_at"], errors="coerce"
        )
        self._log("info", "Parsed extracted_at to datetime")
        return df

    def _fill_missing(self, df):
        fill_map = {
            "temperature":   df["temperature"].median(),
            "humidity":      df["humidity"].median(),
            "wind_speed":    0.0,
            "precipitation": 0.0,
        }
        for col, fill_val in fill_map.items():
            if col in df.columns:
                temp = df[col].isna().sum()
                if temp > 0:
                    self._log("info", f"Filling {temp} null(s) in '{col}' with {fill_val}")
                    df[col] = df[col].fillna(fill_val)
        return df

    def _add_weather_description(self, df):

        code = df["weather_code"]

        conditions = [
            code == 0,
            code.between(1, 3),
            code.between(45, 48),
            code.between(51, 67),
            code.between(71, 77),
            code.between(80, 82),
            code.between(95, 99),
        ]
        descriptions = [
            "Clear sky",
            "Partly cloudy",
            "Foggy",
            "Drizzle / Rain",
            "Snow",
            "Rain showers",
            "Thunderstorm",
        ]

        df["weather_description"] = np.select(
            conditions,
            descriptions,
            default="Unknown"
        )
        return df

    def _add_heat_index(self, df):

        T = df["temperature"].to_numpy()
        H = df["humidity"].to_numpy()

        # Steadman heat index formula (simplified) | Rothfusz Heat Index
        heat_index = (
            -8.78469475556
            + 1.61139411 * T
            + 2.3385491 * H
            - 0.14611605 * T * H
            - 0.012308094 * T**2
            - 0.016424828 * H**2
            + 0.002211732 * T**2 * H
            + 0.00072546 * T * H**2
            - 0.000003582 * T**2 * H**2
        )

        df["heat_index"] = np.round(heat_index, 2)

        # comfort label using np.select again
        conditions = [
            df["heat_index"] <= 27,
            df["heat_index"].between(27, 32),
            df["heat_index"].between(32, 41),
            df["heat_index"].between(41, 54),
            df["heat_index"] > 54,
        ]

        labels = ["Comfortable", "Caution", "Extreme Caution",
                  "Danger", "Extreme Danger"]
        df["comfort_level"] = np.select(conditions, labels, default="Unknown")

        self._log("info", "Added heat_index and comfort_level columns")
        return df

    def _add_pipeline_metadata(self, df):
        df["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["pipeline_version"] = "1.0.0"
        return df

    def _reorder_columns(self, df):
        order = [
            "location", "latitude", "longitude",
            "extracted_at", "processed_at",
            "temperature", "humidity", "wind_speed",
            "precipitation", "weather_code",
            "weather_description", "heat_index", "comfort_level",
            "pipeline_version",
        ]
        # only keep columns that exist
        final_cols = [c for c in order if c in df.columns]
        return df[final_cols]

    def _log_summary(self, df):
        s = self._summary
        self._log("success",
            f"Transform complete — in: {s['total_in']} | "
            f"dropped: {s['dropped']} | out: {s['total_out']}"
        )
        # quick stats using pandas describe()
        print("\n── DataFrame summary ─────────────────────")
        print(df[["temperature", "humidity",
                   "wind_speed", "heat_index"]].describe().round(2))
        print("──────────────────────────────────────────\n")