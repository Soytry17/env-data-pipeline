from abc import ABC, abstractmethod
from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()


class BaseExtractor(ABC):
    def __init__(self, source, logger=None):
        self.source = source
        self.logger = logger

    def _log(self, level, message):
        if self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    @abstractmethod
    def extract(self):
        """Must return a list of dicts"""
        pass

    def __str__(self):
        return f"{self.__class__.__name__} → {self.source}"


class APIExtractor(BaseExtractor):
    # locations to extract
    LOCATIONS = [
        {"name": "Phnom Penh", "latitude": 11.5564, "longitude": 104.9282},
        {"name": "Siem Reap", "latitude": 13.3671, "longitude": 103.8448},
        {"name": "Sihanoukville", "latitude": 10.6333, "longitude": 103.5000},
    ]

    BASE_URL = os.getenv("BASE_URL")

    PARAMS = {
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
            "precipitation",
            "weather_code",
        ],
        "timezone": "Asia/Phnom_Penh",
    }

    def extract(self):

        rows = []

        for location in self.LOCATIONS:

            try:

                row = self._fetch_location(location)
                if row:
                    rows.append(row)

            except Exception as e:

                self._log("warning", f"Failed to fetch '{location['name']}': {e}")

        self._log("info", f"Extracted {len(rows)} location(s) from Open-Meteo API")
        return rows

    def _fetch_location(self, location):
        params = {
            **self.PARAMS,
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "current": ",".join(self.PARAMS["current"]),
        }

        # request data from API
        response = requests.get(str(self.BASE_URL), params=params, timeout=10)
        response.raise_for_status()

        # receive data from response
        raw = response.json()
        current = raw.get("current", {})

        # flatten into one clean dict
        row = {
            "location": location["name"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "extracted_at": current.get("time"),
            "temperature": current.get("temperature_2m"),
            "humidity": current.get("relative_humidity_2m"),
            "wind_speed": current.get("wind_speed_10m"),
            "precipitation": current.get("precipitation"),
            "weather_code": current.get("weather_code"),
        }

        self._log("info",
                  f"  fetched '{location['name']}' → temp: {row['temperature']}°C  humidity: {row['humidity']}%")
        return row
