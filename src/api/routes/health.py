"""
Health-check routes for the Healthcare AI Platform API.

Provides lightweight endpoints for verifying that the FastAPI service
is running and accepting requests.
"""

from fastapi import APIRouter


router = APIRouter()


@router.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "healthcare-ai-platform",
    }