import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Union


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
)


class BronzeWriter:
    def __init__(self, base_path: Union[str, Path] = DEFAULT_BRONZE_PATH):
        self.base_path = Path(base_path)

    def write_entries(
        self,
        resource_type: str,
        entries: List[Dict[str, Any]],
        source_url: str,
    ) -> Path:
        extracted_at = datetime.now(timezone.utc)
        run_id = extracted_at.strftime("%Y%m%dT%H%M%SZ")

        run_folder = (
            self.base_path
            / resource_type.lower()
            / extracted_at.strftime("%Y-%m-%d")
            / run_id
        )
        run_folder.mkdir(parents=True, exist_ok=True)

        bundle = {
            "resourceType": "Bundle",
            "type": "collection",
            "entry": entries,
        }

        metadata = {
            "resource_type": resource_type,
            "extracted_at": extracted_at.isoformat(),
            "record_count": len(entries),
            "status": "success",
            "source_url": source_url,
            "run_id": run_id,
        }

        bundle_path = run_folder / "bundle.json"
        metadata_path = run_folder / "metadata.json"

        self._write_json(bundle_path, bundle)
        self._write_json(metadata_path, metadata)

        return bundle_path

    @staticmethod
    def _write_json(path: Path, content: Dict[str, Any]) -> None:
        with path.open("w", encoding="utf-8") as file:
            json.dump(content, file, indent=2)
