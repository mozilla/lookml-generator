"""Utils for operational monitoring."""
from typing import Any, Dict, List

from google.cloud import bigquery

from .constants import OPMON_DASH_EXCLUDED_FIELDS, OPMON_EXCLUDED_FIELDS
from .views import lookml_utils


def compute_opmon_dimensions(
    bq_client: bigquery.Client, table: str
) -> List[Dict[str, Any]]:
    """
    Compute dimensions for Operational Monitoring.

    For a given Operational Monitoring dimension, find its default (most common)
    value and its top 10 most common to be used as dropdown options.
    """
    all_dimensions = lookml_utils._generate_dimensions(bq_client, table)
    copy_excluded = OPMON_EXCLUDED_FIELDS.copy()
    copy_excluded.update(OPMON_DASH_EXCLUDED_FIELDS)
    dimensions = []

    relevant_dimensions = [
        dimension
        for dimension in all_dimensions
        if dimension["name"] not in copy_excluded
    ]

    for dimension in relevant_dimensions:
        dimension_name = dimension["name"]
        query_job = bq_client.query(
            f"""
                    SELECT DISTINCT {dimension_name}, COUNT(*)
                    FROM {table}
                    GROUP BY 1
                    ORDER BY 2 DESC
                """
        )

        title = lookml_utils.slug_to_title(dimension_name)
        dimension_options = query_job.result().to_dataframe()[dimension_name].tolist()

        dimension_kwarg = {
            "title": title,
            "name": dimension_name,
        }

        if len(dimension_options) > 0:
            dimension_kwarg.update(
                {
                    "default": dimension_options[0],
                    "options": dimension_options[:10],
                }
            )

        dimensions.append(dimension_kwarg)

    return dimensions
