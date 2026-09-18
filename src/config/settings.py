import os

from dotenv import load_dotenv


load_dotenv()

FHIR_BASE_URL = os.getenv(
    "FHIR_BASE_URL",
    "http://localhost:8080/fhir",
)

PGVECTOR_HOST = os.getenv(
    "PGVECTOR_HOST",
    "localhost",
)

PGVECTOR_PORT = int(
    os.getenv("PGVECTOR_PORT", "5432")
)

PGVECTOR_DATABASE = os.getenv(
    "PGVECTOR_DATABASE",
    "hapi",
)

PGVECTOR_USER = os.getenv(
    "PGVECTOR_USER",
    "admin",
)

PGVECTOR_PASSWORD = os.getenv(
    "PGVECTOR_PASSWORD",
    "admin",
)

DEFAULT_HEADERS = {
    "Accept": "application/fhir+json",
    "Content-Type": "application/fhir+json",
}

DEBUG_LOGGING = True