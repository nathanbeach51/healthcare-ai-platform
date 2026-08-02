from ingestion.fhir_client import FhirClient


client = FhirClient()

metadata = client.health_check()

print(metadata["resourceType"])

patients = client.search(
    "Patient",
    params={"_count": 5}
)

entries = client.get_entries(patients)

print(f"Patients returned: {len(entries)}")

first_patient = entries[0]["resource"]

print(first_patient["id"])