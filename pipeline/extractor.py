from abc import ABC, abstractmethod
import requests
import os
import sys
import traceback

from repository.location_repository import get_active_locations
from repository.source_repository import get_source

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class BaseExtractor(ABC):
    def __init__(self, source_id, logger=None):
        self.source_id = source_id
        self.logger    = logger

    def _log(self, level, message):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    @abstractmethod
    def extract(self):
        pass

    def __str__(self):
        return f"{self.__class__.__name__} → source_id: {self.source_id}"


class APIExtractor(BaseExtractor):
    # Open-Meteo fields to request
    CURRENT_PARAMS = [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "precipitation",
        "weather_code",
        "surface_pressure",
        "cloud_cover",
        "visibility",
        "wind_gusts_10m",
    ]

    def __init__(self, source_id, logger=None):
        super().__init__(source_id, logger)
        self.source_config = get_source(source_id)

        if not self.source_config:
            raise ValueError(f"No active source found: {source_id}")

        self.active_locations = get_active_locations(source_id)

        self._log(
            "info",
            f"Loaded {len(self.active_locations)} locations "
            f"for {self.source_config['source_name']}"
        )

    def extract(self):
        if not self.active_locations:
            self._log("warning", "No active locations found — nothing to extract")
            return []

        rows        = []
        failed      = []

        for location in self.active_locations:
            try:
                # append location into rows
                row = self._fetch_location(location)
                if row:
                    rows.append(row)
            except Exception as e:
                self._log("warning",
                    f"Failed to fetch '{location['name']}': "
                    f"[{type(e).__name__}] {e!r}"
                )
                self._log("warning", traceback.format_exc())
                failed.append(location["name"])

        self._log("info",
            f"Extracted {len(rows)}/{len(self.active_locations)} "
            f"location(s) successfully"
        )

        if failed:
            self._log("warning", f"Failed locations: {', '.join(failed)}")

        return rows

    def _fetch_location(self, location):
        # merge default params with any custom_params from config
        custom = location.get("custom_params", {})
        extra_fields = custom.get("extra_fields", [])
        all_fields   = self.CURRENT_PARAMS + extra_fields

        params = {
            "latitude":  location["latitude"],
            "longitude": location["longitude"],
            "current":   ",".join(all_fields),
            "timezone":  location["timezone"],
        }

        response = requests.get(
            self.source_config["api_url"],
            params  = params,
            timeout = self.source_config["timeout_sec"]
        )


        response.raise_for_status()

        raw_response = response.json()

        # observation_at comes from the API response itself
        observation_at = (
            raw_response
            .get("current", {})
            .get("time")
        )

        row = {
            "source_id":    self.source_id,
            "location_id":  location["location_id"],
            "location_name": location["name"],        # for logging only
            "observation_at": observation_at,
            "raw_data":     raw_response,
        }


        self._log("info",
            f"  ✔ {location['name']:<22} "
            f"({location['region']:<10}) "
            f"temp: {raw_response.get('current', {}).get('temperature_2m')}°C"
        )

        return row

    # ─── HELPERS

    def get_location_count(self):
        return len(self.active_locations)

    def get_source_name(self):
        return self.source_config["source_name"]

    def __str__(self):
        return (
            f"APIExtractor → "
            f"{self.source_config['source_name']} · "
            f"{len(self.active_locations)} locations"
        )