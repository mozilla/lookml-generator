"""Utils for generating lookml."""
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import click
from google.cloud import bigquery
from jinja2 import Environment, PackageLoader

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
    if (
        mode == "REPEATED"
        or path in HIDDEN_DIMENSIONS
        or field_type not in BIGQUERY_TYPE_TO_DIMENSION_TYPE
    ):
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


def _generate_nested_dimension_views(
    schema: List[bigquery.SchemaField], view_name: str
) -> List[Dict[str, Any]]:
    """
    Recursively generate views for nested fields.

    Nested fields are created as views, with dimensions and optionally measures.
    """
    views: List[Dict[str, Any]] = []
    for field in sorted(schema, key=lambda f: f.name):
        if field.field_type == "RECORD" and field.name != "labeled_counter":
            # labeled_counter is handled explicitly in glean ping views; hidden for other views
            if field.mode == "REPEATED":
                nested_field_view: Dict[str, Any] = {
                    "name": f"{view_name}__{field.name}"
                }
                dimensions = _generate_dimensions_helper(schema=field.fields)
                nested_field_view["dimensions"] = [
                    d for d in dimensions if not _is_dimension_group(d)
                ]
                nested_field_view["dimension_groups"] = [
                    d for d in dimensions if _is_dimension_group(d)
                ]
                views = (
                    views
                    + [nested_field_view]
                    + _generate_nested_dimension_views(
                        field.fields, f"{view_name}__{field.name}"
                    )
                )
            else:
                views = views + _generate_nested_dimension_views(
                    field.fields, f"{view_name}__{field.name}"
                )

    return views


def _is_dimension_group(dimension: dict):
    """Determine if a dimension is actually a dimension group."""
    return "timeframes" in dimension or "intervals" in dimension


def escape_filter_expr(expr: str) -> str:
    """Escape filter expression for special Looker chars."""
    return re.sub(r'((?:^-)|["_%,^])', r"^\1", expr, count=0)


def _is_nested_dimension(dimension: dict):
    return (
        "hidden" in dimension
        and dimension["hidden"]
        and "nested" in dimension
        and dimension["nested"]
    )


def render_template(filename, template_folder, **kwargs) -> str:
    """Render a given template using Jinja."""
    env = Environment(loader=PackageLoader("generator", f"{template_folder}/templates"))
    template = env.get_template(filename)
    rendered = template.render(**kwargs)
    return rendered


def get_distinct_vals(bq_client: bigquery.Client, table: str, column: str):
    """Given a table and column name, return all distinct values for that column."""
    query_job = bq_client.query(
        f"""
            SELECT DISTINCT {column}
            FROM {table}
        """
    )
    distinct_values = query_job.result().to_dataframe()[column].tolist()
    return distinct_values


def slug_to_title(slug):
    """Convert a slug to title case."""
    return slug.replace("_", " ").title()
