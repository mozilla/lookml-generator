"""Constants."""
from typing import List, Set


# These are fields we don't need for opmon dashboards
OPMON_DASH_EXCLUDED_FIELDS: List[str] = [
    "branch",
    "probe",
    "histogram__VALUES__key",
    "histogram__VALUES__value",
]
