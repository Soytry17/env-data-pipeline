from fastapi import APIRouter, Depends, Query
from typing import Optional
from datetime import date
from api.controllers.weather_controller import WeatherController

router = APIRouter()


@router.get("/weather/latest", tags=["Weather"])
def latest(
    location:    Optional[str] = Query(None),
    controller:  WeatherController = Depends()
):
    return controller.latest(location)


@router.get("/weather/history", tags=["Weather"])
def history(
    location:    str            = Query(...),
    date_from:   Optional[date] = Query(None),
    date_to:     Optional[date] = Query(None),
    limit:       int            = Query(100),
    controller:  WeatherController = Depends()
):
    return controller.history(location, date_from, date_to, limit)


@router.get("/weather/summary", tags=["Weather"])
def summary(
    location:    Optional[str]  = Query(None),
    date_from:   Optional[date] = Query(None),
    date_to:     Optional[date] = Query(None),
    controller:  WeatherController = Depends()
):
    return controller.summary(location, date_from, date_to)


@router.get("/health", tags=["Health"])
def health(controller: WeatherController = Depends()):
    return controller.health()