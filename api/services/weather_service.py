from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import date

class WeatherService:

    def __init__(self, db: Session):
        self.db = db

    def get_latest(self, location: Optional[str] = None):
        where  = "WHERE location ILIKE :location" if location else ""
        params = {"location": f"%{location}%"} if location else {}

        sql = text(f"""
            SELECT DISTINCT ON (location)
                location, extracted_at, temperature_c,
                humidity_pct, wind_speed_kmh, precipitation_mm,
                weather_description, heat_index_c, comfort_level
            FROM silver.stg_weather
            {where}
            ORDER BY location, extracted_at DESC
        """)
        return self.db.execute(sql, params).mappings().all()

    def get_history(
        self,
        location:  str,
        date_from: Optional[date] = None,
        date_to:   Optional[date] = None,
        limit:     int = 100
    ):
        filters = ["location ILIKE :location"]
        params  = {"location": f"%{location}%", "limit": limit}

        if date_from:
            filters.append("DATE(extracted_at) >= :date_from")
            params["date_from"] = date_from
        if date_to:
            filters.append("DATE(extracted_at) <= :date_to")
            params["date_to"] = date_to

        sql = text(f"""
            SELECT
                location, extracted_at, temperature_c,
                humidity_pct, wind_speed_kmh, precipitation_mm,
                weather_description, comfort_level
            FROM silver.stg_weather
            WHERE {' AND '.join(filters)}
            ORDER BY extracted_at DESC
            LIMIT :limit
        """)
        return self.db.execute(sql, params).mappings().all()

    def get_summary(
        self,
        location:  Optional[str]  = None,
        date_from: Optional[date] = None,
        date_to:   Optional[date] = None,
    ):
        filters = []
        params  = {}

        if location:
            filters.append("location ILIKE :location")
            params["location"] = f"%{location}%"
        if date_from:
            filters.append("date >= :date_from")
            params["date_from"] = date_from
        if date_to:
            filters.append("date <= :date_to")
            params["date_to"] = date_to

        where = ("WHERE " + " AND ".join(filters)) if filters else ""

        sql = text(f"""
            SELECT
                location, date, avg_temp_c, min_temp_c, max_temp_c,
                avg_humidity_pct, total_precipitation_mm,
                avg_wind_speed_kmh, avg_heat_index_c,
                dominant_comfort_level, day_classification, total_readings
            FROM gold.agg_daily_weather
            {where}
            ORDER BY date DESC, location
        """)
        return self.db.execute(sql, params).mappings().all()

    def get_health_stats(self):
        return {
            "bronze_rows":  self.db.execute(
                text("SELECT COUNT(*) FROM bronze.weather")
            ).scalar() or 0,
            "silver_rows":  self.db.execute(
                text("SELECT COUNT(*) FROM silver.stg_weather")
            ).scalar() or 0,
            "gold_rows":    self.db.execute(
                text("SELECT COUNT(*) FROM gold.agg_daily_weather")
            ).scalar() or 0,
            "last_ingested": self.db.execute(
                text("SELECT MAX(created_at) FROM bronze.weather")
            ).scalar(),
        }