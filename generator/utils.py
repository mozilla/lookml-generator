"""Utils."""

import io
import urllib.request
import zipfile
from pathlib import Path

OWNER, REPO, REF = "mozilla", "looker-hub", "main"  # or a pinned SHA
CODELOAD = (
    f"https://codeload.github.com/{OWNER}/{REPO}/zip/refs/heads/{REF}"
    if len(REF) < 40
    else f"https://codeload.github.com/{OWNER}/{REPO}/zip/{REF}"
)


def get_file_from_looker_hub(path: Path):
    file = path.name
    artifact_type = path.parent.name
    namespace = path.parent.parent.name

    print(CODELOAD)
    with urllib.request.urlopen(CODELOAD, timeout=60) as r:
        data = r.read()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        root = zf.namelist()[0].split("/")[0]  # e.g., "looker-hub-main"
        inner = f"{root}/{namespace}/{artifact_type}/{file}"
        with zf.open(inner) as fh:
            content = fh.read().decode("utf-8", errors="replace")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
