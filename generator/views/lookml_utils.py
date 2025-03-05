"""Utils for generating lookml."""

import re
import tarfile
import urllib.request
from collections import defaultdict
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import click
import yaml
from jinja2 import Environment, FileSystemLoader
from generator.namespaces import DEFAULT_GENERATED_SQL_URI

GENERATOR_PATH = Path(__file__).parent.parent

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

DEFAULT_MAX_SUGGEST_PERSIST_FOR = "24 hours"


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
        result["suggest_persist_for"] = DEFAULT_MAX_SUGGEST_PERSIST_FOR

        group_label, group_item_label = None, None
        if len(path) > 1:
            group_label = slug_to_title(" ".join(path[:-1]))
            group_item_label = slug_to_title(path[-1])
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


def _generate_dimensions_helper(schema: List[Any], *prefix: str) -> Iterable[dict]:
    for field in sorted(schema, key=lambda f: f["name"]):
        if field["type"] == "RECORD" and not field.get("mode", "") == "REPEATED":
            yield from _generate_dimensions_helper(
                field["fields"], *prefix, field["name"]
            )
        else:
            yield _get_dimension(
                (*prefix, field["name"]),
                field["type"],
                field.get("mode", ""),
                field.get("description", ""),
            )


def _generate_dimensions(table: str, dryrun) -> List[Dict[str, Any]]:
    """Generate dimensions and dimension groups from a bigquery table.

    When schema contains both submission_timestamp and submission_date, only produce
    a dimension group for submission_timestamp.

    Raise ClickException if schema results in duplicate dimensions.
    """
    dimensions = {}
    [project, dataset, table] = table.split(".")
    table_schema = dryrun.create(
        project=project,
        dataset=dataset,
        table=table,
    ).get_table_schema()

    for dimension in _generate_dimensions_helper(table_schema):
        name_key = dimension["name"]

        # This prevents `time` dimension groups from overwriting other dimensions below
        if dimension.get("type") == "time":
            name_key += "_time"
        # overwrite duplicate "submission", "end", "start" dimension group, thus picking the
        # last value sorted by field name, which is submission_timestamp
        # See also https://github.com/mozilla/lookml-generator/issues/471
        if name_key in dimensions and not (
            dimension.get("type") == "time"
            and (
                dimension["name"] == "submission"
                or dimension["name"].endswith("end")
                or dimension["name"].endswith("start")
            )
        ):
            raise click.ClickException(
                f"duplicate dimension {name_key!r} for table {table!r}"
            )
        dimensions[name_key] = dimension
    return list(dimensions.values())


def _generate_dimensions_from_query(query: str, dryrun) -> List[Dict[str, Any]]:
    """Generate dimensions and dimension groups from a SQL query."""
    schema = dryrun.create(sql=query).get_schema()
    dimensions = {}
    for dimension in _generate_dimensions_helper(schema or []):
        name_key = dimension["name"]

        # This prevents `time` dimension groups from overwriting other dimensions below
        if dimension.get("type") == "time":
            name_key += "_time"

        # overwrite duplicate "submission", "end", "start" dimension group, thus picking the
        # last value sorted by field name, which is submission_timestamp
        # See also https://github.com/mozilla/lookml-generator/issues/471
        if name_key in dimensions and not (
            dimension.get("type") == "time"
            and (
                dimension["name"] == "submission"
                or dimension["name"].endswith("end")
                or dimension["name"].endswith("start")
            )
        ):
            raise click.ClickException(f"duplicate dimension {name_key!r} in query")
        dimensions[name_key] = dimension
    return list(dimensions.values())


def _generate_nested_dimension_views(
    schema: List[dict], view_name: str
) -> List[Dict[str, Any]]:
    """
    Recursively generate views for nested fields.

    Nested fields are created as views, with dimensions and optionally measures.
    """
    views: List[Dict[str, Any]] = []
    for field in sorted(schema, key=lambda f: f["name"]):
        if field["type"] == "RECORD" and field["name"] != "labeled_counter":
            # labeled_counter is handled explicitly in glean ping views; hidden for other views
            if field.get("mode") == "REPEATED":
                nested_field_view: Dict[str, Any] = {
                    "name": f"{view_name}__{field['name']}"
                }
                dimensions = _generate_dimensions_helper(schema=field["fields"])
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
                        field["fields"], f"{view_name}__{field['name']}"
                    )
                )
            else:
                views = views + _generate_nested_dimension_views(
                    field["fields"], f"{view_name}__{field['name']}"
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
    env = Environment(
        loader=FileSystemLoader(GENERATOR_PATH / f"{template_folder}/templates")
    )
    template = env.get_template(filename)
    rendered = template.render(**kwargs)
    return rendered


def slug_to_title(slug):
    """Convert a slug to title case."""
    return slug.replace("_", " ").title()


# Map from view to qualified references {dataset: {view: [[project, dataset, table],]}}
BQViewReferenceMap = Dict[str, Dict[str, List[List[str]]]]


def get_bigquery_view_reference_map(
    generated_sql_uri: str,
) -> BQViewReferenceMap:
    """Get a mapping from BigQuery datasets to views with references."""
    with urllib.request.urlopen(generated_sql_uri) as f:
        tarbytes = BytesIO(f.read())
    views: BQViewReferenceMap = defaultdict(dict)
    with tarfile.open(fileobj=tarbytes, mode="r:gz") as tar:
        for tarinfo in tar:
            if tarinfo.name.endswith("/metadata.yaml"):
                metadata = yaml.safe_load(tar.extractfile(tarinfo.name))  # type: ignore
                references = metadata.get("references", {})
                if "view.sql" not in references:
                    continue
                *_, project, dataset_id, view_id, _ = tarinfo.name.split("/")
                if project == "moz-fx-data-shared-prod":
                    views[dataset_id][view_id] = [
                        ref.split(".") for ref in references["view.sql"]
                    ]
    return views


def get_bigquery_table_references(
    dataset_id: str,
    table_id: str,
    dataset_view_map: Optional[BQViewReferenceMap] = None,
) -> List[List[str]]:
    """
    Return referenced tables.

    Extracts referenced tables from referenced views.
    """
    dataset_view_references = dataset_view_map.get(dataset_id)
    if dataset_view_references is None:
        return []

    view_references = dataset_view_references.get(table_id)
    if view_references is None:
        return []

    return {
        ref
        for view_reference in view_references
        for ref in get_bigquery_table_references(view_reference[0], view_reference[1])
    }
