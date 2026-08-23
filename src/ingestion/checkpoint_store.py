import json
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "data"
    / "state"
    / "extract_checkpoints.json"
)


def load_checkpoints() -> dict[str, str]:
    if not CHECKPOINT_PATH.exists():
        return {}

    with CHECKPOINT_PATH.open("r") as file:
        return json.load(file)


def get_checkpoint(
    resource_type: str,
) -> Optional[str]:
    checkpoints = load_checkpoints()

    return checkpoints.get(resource_type)

def save_checkpoint(
    resource_type: str,
    checkpoint: str,
) -> None:
    checkpoints = load_checkpoints()

    checkpoints[resource_type] = checkpoint

    CHECKPOINT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with CHECKPOINT_PATH.open("w") as file:
        json.dump(
            checkpoints,
            file,
            indent=2,
        )