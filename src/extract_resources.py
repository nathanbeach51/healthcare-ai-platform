from datetime import datetime, timezone

from ingestion.checkpoint_store import (
    get_checkpoint,
    save_checkpoint,
)
from ingestion.fhir_client import FhirClient
from storage.bronze_writer import BronzeWriter


FHIR_RESOURCES = [
    "Patient",
    "Encounter",
    "Condition",
    "Observation",
    "MedicationRequest",
]


def extract_resources() -> None:
    client = FhirClient()
    writer = BronzeWriter()

    for resource_type in FHIR_RESOURCES:
        try:
            print(f"Extracting {resource_type}...")

            params = {
                "_count": 100,
            }

            checkpoint = get_checkpoint(
                resource_type
            )

            if checkpoint:
                params["_lastUpdated"] = (
                    f"gt{checkpoint}"
                )

            print(
                f"Extracting {resource_type} "
                f"with params: {params}"
            )

            extract_started_at = datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            entries = client.search_all(
                resource_type,
                params=params,
            )

            if entries:
                path = writer.write_entries(
                    resource_type=resource_type,
                    entries=entries,
                    source_url=client.base_url,
                )

                print(
                    f"Retrieved {len(entries)} "
                    f"{resource_type} resources. "
                    f"Saved to {path}"
                )

            else:
                print(
                    f"No new or updated "
                    f"{resource_type} resources."
                )

            save_checkpoint(
                resource_type,
                extract_started_at,
            )

        except Exception as error:
            print(
                f"Failed to extract "
                f"{resource_type}: {error}"
            )
            raise


if __name__ == "__main__":
    extract_resources()