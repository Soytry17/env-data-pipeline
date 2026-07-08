from fastapi import FastAPI
from api.routes.weather_routes import router

app = FastAPI(
    title       = "Environmental Data Pipeline API",
    description = "Weather data served from Bronze → Silver → Gold layers",
    version     = "1.0.0",
)

app.include_router(router)


@app.get("/", tags=["Root"])
def root():
    return {
        "project":   "Environmental Data Pipeline",
        "version":   "1.0.0",
        "docs":      "/docs",
        "endpoints": [
            "GET /health",
            "GET /weather/latest",
            "GET /weather/history",
            "GET /weather/summary",
        ]
    }