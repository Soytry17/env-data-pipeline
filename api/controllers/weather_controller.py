from fastapi import HTTPException, Depends
from sqlalchemy.orm import Session
from typing import Optional
from datetime import date
from api.database import get_db
from api.services.weather_service import WeatherService
from api.schemas.weather_schema import (
    WeatherLatestResponse,
    WeatherHistoryResponse,
    WeatherSummaryResponse,
    HealthResponse,
)


class WeatherController:

    def __init__(self, db: Session = Depends(get_db)):
        self.service = WeatherService(db)

    def latest(self, location: Optional[str] = None):
        rows = self.service.get_latest(location)
        if not rows:
            raise HTTPException(
                status_code = 404,
                detail      = f"No data found for '{location}'"
                              if location else "No data found"
            )
        return [WeatherLatestResponse(**dict(r)) for r in rows]

    def history(
        self,
        location:  str,
        date_from: Optional[date] = None,
        date_to:   Optional[date] = None,
        limit:     int = 100,
    ):
        if limit < 1 or limit > 1000:
            raise HTTPException(
                status_code = 422,
                detail      = "limit must be between 1 and 1000"
            )
        rows = self.service.get_history(location, date_from, date_to, limit)
        if not rows:
            raise HTTPException(
                status_code = 404,
                detail      = f"No history found for '{location}'"
            )
        return [WeatherHistoryResponse(**dict(r)) for r in rows]

    def summary(
        self,
        location:  Optional[str]  = None,
        date_from: Optional[date] = None,
        date_to:   Optional[date] = None,
    ):
        rows = self.service.get_summary(location, date_from, date_to)
        if not rows:
            raise HTTPException(status_code=404, detail="No summary data found")
        return [WeatherSummaryResponse(**dict(r)) for r in rows]

    def health(self):
        try:
            stats = self.service.get_health_stats()
            return HealthResponse(
                status        = "healthy",
                database      = "connected",
                **stats
            )
        except Exception as e:
            return HealthResponse(
                status        = "unhealthy",
                database      = f"error: {str(e)}",
                bronze_rows   = 0,
                silver_rows   = 0,
                gold_rows     = 0,
                last_ingested = None,
            )