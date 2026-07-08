from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date


class LatestQuery(BaseModel):
    location: Optional[str] = None


class HistoryQuery(BaseModel):
    location:  str
    date_from: Optional[date] = None
    date_to:   Optional[date] = None
    limit:     int = 100


class SummaryQuery(BaseModel):
    location:  Optional[str] = None
    date_from: Optional[date] = None
    date_to:   Optional[date] = None


class WeatherLatestResponse(BaseModel):
    location:            str
    extracted_at:        Optional[datetime]
    temperature_c:       Optional[float]
    humidity_pct:        Optional[float]
    wind_speed_kmh:      Optional[float]
    precipitation_mm:    Optional[float]
    weather_description: Optional[str]
    heat_index_c:        Optional[float]
    comfort_level:       Optional[str]

    class Config:
        from_attributes = True


class WeatherHistoryResponse(BaseModel):
    location:            str
    extracted_at:        Optional[datetime]
    temperature_c:       Optional[float]
    humidity_pct:        Optional[float]
    wind_speed_kmh:      Optional[float]
    precipitation_mm:    Optional[float]
    weather_description: Optional[str]
    comfort_level:       Optional[str]

    class Config:
        from_attributes = True


class WeatherSummaryResponse(BaseModel):
    location:               str
    date:                   date
    avg_temp_c:             Optional[float]
    min_temp_c:             Optional[float]
    max_temp_c:             Optional[float]
    avg_humidity_pct:       Optional[float]
    total_precipitation_mm: Optional[float]
    avg_wind_speed_kmh:     Optional[float]
    avg_heat_index_c:       Optional[float]
    dominant_comfort_level: Optional[str]
    day_classification:     Optional[str]
    total_readings:         Optional[int]

    class Config:
        from_attributes = True


class HealthResponse(BaseModel):
    status:        str
    database:      str
    bronze_rows:   int
    silver_rows:   int
    gold_rows:     int
    last_ingested: Optional[datetime]