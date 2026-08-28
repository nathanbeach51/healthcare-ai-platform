import os

FHIR_BASE_URL = os.getenv(
    "FHIR_BASE_URL",
    "http://localhost:8080/fhir",
)

DEFAULT_HEADERS = {
    "Accept": "application/fhir+json",
    "Content-Type": "application/fhir+json"
}

DEBUG_LOGGING = False