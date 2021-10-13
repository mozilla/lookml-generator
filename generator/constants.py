"""Constants."""
from typing import Set

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
