# api/models/weather.py

from sqlalchemy import Column, Integer, String, Float, DateTime, Numeric
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime


class Base(DeclarativeBase):
    pass


class BronzeWeather(Base):
    __tablename__ = "weather"
    __table_args__ = {"schema": "bronze"}

    id                  = Column(Integer, primary_key=True)
    location            = Column(String(100), nullable=False)
    latitude            = Column(Numeric(9, 6))
    longitude           = Column(Numeric(9, 6))
    extracted_at        = Column(DateTime)
    processed_at        = Column(DateTime)
    temperature         = Column(Numeric(5, 2))
    humidity            = Column(Numeric(5, 2))
    wind_speed          = Column(Numeric(6, 2))
    precipitation       = Column(Numeric(6, 2))
    weather_code        = Column(Integer)
    weather_description = Column(String(100))
    heat_index          = Column(Numeric(5, 2))
    comfort_level       = Column(String(50))
    pipeline_version    = Column(String(20))
    created_at          = Column(DateTime, default=datetime.now)


class SilverWeather(Base):
    __tablename__ = "stg_weather"
    __table_args__ = {"schema": "silver"}

    id                  = Column(Integer, primary_key=True)
    location            = Column(String(100))
    latitude            = Column(Numeric(9, 6))
    longitude           = Column(Numeric(9, 6))
    extracted_at        = Column(DateTime)
    processed_at        = Column(DateTime)
    temperature_c       = Column(Numeric(5, 2))
    humidity_pct        = Column(Numeric(5, 2))
    wind_speed_kmh      = Column(Numeric(6, 2))
    precipitation_mm    = Column(Numeric(6, 2))
    weather_code        = Column(Integer)
    weather_description = Column(String(100))
    heat_index_c        = Column(Numeric(5, 2))
    comfort_level       = Column(String(50))
    reading_date        = Column(DateTime)
    reading_hour        = Column(Integer)


class GoldDailyWeather(Base):
    __tablename__ = "agg_daily_weather"
    __table_args__ = {"schema": "gold"}

    location                = Column(String(100), primary_key=True)
    date                    = Column(DateTime,    primary_key=True)
    avg_temp_c              = Column(Numeric(5, 2))
    min_temp_c              = Column(Numeric(5, 2))
    max_temp_c              = Column(Numeric(5, 2))
    avg_humidity_pct        = Column(Numeric(5, 2))
    min_humidity_pct        = Column(Numeric(5, 2))
    max_humidity_pct        = Column(Numeric(5, 2))
    avg_wind_speed_kmh      = Column(Numeric(6, 2))
    max_wind_speed_kmh      = Column(Numeric(6, 2))
    total_precipitation_mm  = Column(Numeric(6, 2))
    avg_heat_index_c        = Column(Numeric(5, 2))
    max_heat_index_c        = Column(Numeric(5, 2))
    dominant_comfort_level  = Column(String(50))
    total_readings          = Column(Integer)
    hours_covered           = Column(Integer)
    day_classification      = Column(String(50))