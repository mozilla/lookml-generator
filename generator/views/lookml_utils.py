"""Utils for generating lookml."""
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import click
from google.cloud import bigquery

BIGQUERY_TYPE_TO_DIMENSION_TYPE = {
    "BIGNUMERIC": "string",
    "BOOLEAN": "yesno",
    "BYTES": "string",
    "DATE": "time",
    "DATETIME": "time",
    "FLOAT": "number",
    "INTEGER": "number",
    "NUMERIC": "number",
    "STRING": "string",
    "TIME": "time",
    "TIMESTAMP": "time",
}

HIDDEN_DIMENSIONS = {
    ("document_id",),
    ("client_id",),
    ("client_info", "client_id"),
    ("context_id",),
    ("additional_properties",),
}

MAP_LAYER_NAMES = {
    ("country",): "countries",
    ("metadata", "geo", "country"): "countries",
}


def _get_dimension(
    path: Tuple[str, ...], field_type: str, mode: str, description: Optional[str]
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    result["sql"] = "${TABLE}." + ".".join(path)
    name = path
    if mode == "REPEATED" or path in HIDDEN_DIMENSIONS:
        result["hidden"] = "yes"
    else:
        result["type"] = BIGQUERY_TYPE_TO_DIMENSION_TYPE[field_type]

        group_label, group_item_label = None, None
        if len(path) > 1:
            group_label = " ".join(path[:-1]).replace("_", " ").title()
            group_item_label = path[-1].replace("_", " ").title()
        if result["type"] == "time":
            # Remove _{type} suffix from the last path element for dimension group
            # names For example submission_date and submission_timestamp become
            # submission, and metadata.header.parsed_date becomes
            # metadata__header__parsed. This is because the timeframe will add a _{type}
            # suffix to the individual dimension names.
            name = *path[:-1], re.sub("_(date|time(stamp)?)$", "", path[-1])
            result["timeframes"] = [
                "raw",
                "time",
                "date",
                "week",
                "month",
                "quarter",
                "year",
            ]
            if field_type == "DATE":
                result["timeframes"].remove("time")
                result["convert_tz"] = "no"
                result["datatype"] = "date"
            if group_label and group_item_label:
                # Dimension groups should not be nested, see issue #82
                result["label"] = f"{group_label}: {group_item_label}"
        elif len(path) > 1:
            result["group_label"] = group_label
            result["group_item_label"] = group_item_label
        if path in MAP_LAYER_NAMES:
            result["map_layer_name"] = MAP_LAYER_NAMES[path]
    result["name"] = "__".join(name)

    if description:
        result["description"] = description

    return result


def _generate_dimensions_helper(
    schema: List[bigquery.SchemaField], *prefix: str
) -> Iterable[dict]:
    for field in sorted(schema, key=lambda f: f.name):
        if field.field_type == "RECORD" and not field.mode == "REPEATED":
            yield from _generate_dimensions_helper(field.fields, *prefix, field.name)
        else:
            yield _get_dimension(
                (*prefix, field.name), field.field_type, field.mode, field.description
            )


def _generate_dimensions(client: bigquery.Client, table: str) -> List[Dict[str, Any]]:
    """Generate dimensions and dimension groups from a bigquery table.

    When schema contains both submission_timestamp and submission_date, only produce
    a dimension group for submission_timestamp.

    Raise ClickException if schema results in duplicate dimensions.
    """
    dimensions = {}
    for dimension in _generate_dimensions_helper(client.get_table(table).schema):
        name = dimension["name"]
        # overwrite duplicate "submission" dimension group, thus picking the
        # last value sorted by field name, which is submission_timestamp
        if name in dimensions and name != "submission":
            raise click.ClickException(
                f"duplicate dimension {name!r} for table {table!r}"
            )
        dimensions[name] = dimension
    return list(dimensions.values())


def _is_dimension_group(dimension: dict):
    """Determine if a dimension is actually a dimension group."""
    return "timeframes" in dimension or "intervals" in dimension


def escape_filter_expr(expr: str) -> str:
    """Escape filter expression for special Looker chars."""
    return re.sub(r'((?:^-)|["_%,^])', r"^\1", expr, count=0)
