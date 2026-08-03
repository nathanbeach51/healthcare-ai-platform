import json
from datetime import datetime
from pathlib import Path


class BronzeWriter:

    def __init__(self, base_path="data/bronze"):
        self.base_path = Path(base_path)

    def write_bundle(self, resource_type, bundle):

        folder = (
            self.base_path
            / resource_type.lower()
            / datetime.now().strftime("%Y-%m-%d")
        )

        folder.mkdir(parents=True, exist_ok=True)

        filename = (
            folder
            / "bundle.json"
        )

        with open(filename, "w") as f:
            json.dump(bundle, f, indent=2)

        return filename