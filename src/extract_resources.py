from ingestion.fhir_client import FhirClient
from storage.bronze_writer import BronzeWriter

FHIR_RESOURCES = [
    "Patient",
    "Encounter",
    "Condition",
    "Observation",
    "MedicationRequest"
]

def extract_resources() -> None:
    client = FhirClient()
    writer = BronzeWriter()

    for resource_type in FHIR_RESOURCES:
        try:
            print(f"Extracting {resource_type}...")

            entries = client.search_all(
                resource_type,
                params={"_count": 100},
            )

            path = writer.write_entries(
                resource_type=resource_type,
                entries=entries,
                source_url=client.base_url,
            )

            print(
                f"Retrieved {len(entries)} {resource_type} resources. "
                f"Saved to {path}"
            )

        except Exception as error:
            print(f"Failed to extract {resource_type}: {error}")

if __name__ == "__main__":
    extract_resources()
