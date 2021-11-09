"""Constants."""
from typing import List, Set

# These are fields we don't need for opmon views/explores/dashboards
OPMON_EXCLUDED_FIELDS: Set[str] = {
    "submission",
    "client_id",
    "build_id",
    "agg_type",
    "value",
    "histogram__VALUES",
    "histogram__bucket_count",
    "histogram__histogram_type",
    "histogram__range",
    "histogram__sum",
}

# These are fields we don't need for opmon dashboards
OPMON_DASH_EXCLUDED_FIELDS: List[str] = [
    "branch",
    "probe",
    "histogram__VALUES__key",
    "histogram__VALUES__value",
]
