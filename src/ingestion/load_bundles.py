from pathlib import Path
import json

import requests

FHIR_URL = "http://localhost:8080/fhir"

FHIR_DIRECTORY = Path(
    "tools/synthea/source/output/fhir"
)

def load_bundle(bundle_file: Path) -> bool:
    with bundle_file.open() as f:
        bundle = json.load(f)

    response = requests.post(
        FHIR_URL,
        json=bundle,
        headers={
            "Content-Type": "application/fhir+json"
        },
    )

    return response.ok

def main():

    bundle_files = sorted(
        FHIR_DIRECTORY.glob("*.json")
    )

    print(f"Found {len(bundle_files)} bundles.")

    success = 0

    for bundle in bundle_files:

        if load_bundle(bundle):
            success += 1
            print(f"✓ {bundle.name}")
            print(bundle.get("total"))
            print(len(bundle.get("entry", [])))
            print(bundle.get("link", []))
        else:
            print(f"✗ {bundle.name}")

    print()
    print(f"Loaded {success} bundles.")

if __name__ == "__main__":
    main()