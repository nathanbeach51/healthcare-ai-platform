from pathlib import Path
import json

import requests


FHIR_URL = "http://localhost:8080/fhir"

FHIR_DIRECTORY = Path(
    "tools/synthea/source/output/fhir"
)

PATIENT_LIMIT = 25


def load_bundle(
    bundle_file: Path,
) -> bool:
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


def load_bundle_group(
    label: str,
    bundle_files: list[Path],
) -> int:
    print(f"\nLoading {label}...")

    success_count = 0

    for bundle_file in bundle_files:
        if load_bundle(bundle_file):
            success_count += 1
            print(f"✓ {bundle_file.name}")
        else:
            print(f"✗ {bundle_file.name}")

    print(
        f"{label}: "
        f"{success_count}/{len(bundle_files)} loaded."
    )

    return success_count


def main() -> None:
    all_bundles = sorted(
        FHIR_DIRECTORY.glob("*.json")
    )

    hospital_bundles = [
        path
        for path in all_bundles
        if path.name.startswith("hospitalInformation")
    ]

    provider_bundles = [
        path
        for path in all_bundles
        if path.name.startswith("practitionerInformation")
    ]

    patient_bundles = [
        path
        for path in all_bundles
        if path not in hospital_bundles
        and path not in provider_bundles
    ]

    patient_bundles = patient_bundles[
        :PATIENT_LIMIT
    ]

    print(
        f"Found {len(hospital_bundles)} "
        "hospital bundles."
    )

    print(
        f"Found {len(provider_bundles)} "
        "provider bundles."
    )

    print(
        f"Loading {len(patient_bundles)} "
        "patient bundles."
    )

    hospital_success = load_bundle_group(
        "Hospital bundles",
        hospital_bundles,
    )

    provider_success = load_bundle_group(
        "Provider bundles",
        provider_bundles,
    )

    patient_success = load_bundle_group(
        "Patient bundles",
        patient_bundles,
    )

    print("\nLoad complete")
    print("-------------")
    print(
        f"Hospital bundles: "
        f"{hospital_success}"
    )
    print(
        f"Provider bundles: "
        f"{provider_success}"
    )
    print(
        f"Patient bundles: "
        f"{patient_success}"
    )


if __name__ == "__main__":
    main()