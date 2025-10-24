"""Utils."""

import os
import urllib.request
from pathlib import Path

LOOKER_HUB_URL = "https://raw.githubusercontent.com/mozilla/looker-hub/main"

LOOKER_HUB_DIR = os.getenv("LOOKER_HUB_DIR")


def get_file_from_looker_hub(path: Path):
    """Download a specific lookml artifact from looker-hub."""
    file = path.name
    artifact_type = path.parent.name
    namespace = path.parent.parent.name
    if LOOKER_HUB_DIR:
        src = Path(LOOKER_HUB_DIR) / namespace / artifact_type / file
        print(f"local: {src}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    else:
        print(f"{LOOKER_HUB_URL}/{namespace}/{artifact_type}/{file}")
        with urllib.request.urlopen(
            f"{LOOKER_HUB_URL}/{namespace}/{artifact_type}/{file}"
        ) as response:
            lookml = response.read().decode(response.headers.get_content_charset())
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(lookml)
