"""
FastAPI application entry point for the Healthcare AI Platform.

Creates the API application and registers route modules for health checks,
patient data access, and AI-assisted patient questions.

Run locally with:

    PYTHONPATH=src uvicorn api.main:app --reload
"""

from fastapi import FastAPI

from api.routes.health import router as health_router
from api.routes.patients import router as patients_router

app = FastAPI(
    title="Healthcare AI Platform API",
    version="0.1.0",
)

app.include_router(health_router)
app.include_router(patients_router)