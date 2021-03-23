"""Generate lookml from namespaces."""
import logging
import re
from itertools import filterfalse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import click
import lkml
import yaml
from google.cloud import bigquery

from .explores import explore_types

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
}

MAP_LAYER_NAMES = {
    ("country",): "countries",
    ("metadata", "geo", "country"): "countries",
}


def _get_dimension(path: Tuple[str, ...], field_type: str, mode: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    result["sql"] = "${TABLE}." + ".".join(path)
    name = path
    if mode == "REPEATED" or path in HIDDEN_DIMENSIONS:
        result["hidden"] = "yes"
    else:
        result["type"] = BIGQUERY_TYPE_TO_DIMENSION_TYPE[field_type]
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
        if len(path) > 1:
            result["group_label"] = " ".join(path[:-1]).replace("_", " ").title()
            result["group_item_label"] = path[-1].replace("_", " ").title()
        if path in MAP_LAYER_NAMES:
            result["map_layer_name"] = MAP_LAYER_NAMES[path]
    result["name"] = "__".join(name)
    return result


def _generate_dimensions_helper(
    schema: List[bigquery.SchemaField], *prefix: str
) -> Iterable[dict]:
    for field in sorted(schema, key=lambda f: f.name):
        if field.field_type == "RECORD" and not field.mode == "REPEATED":
            yield from _generate_dimensions_helper(field.fields, *prefix, field.name)
        else:
            yield _get_dimension((*prefix, field.name), field.field_type, field.mode)


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


def _generate_measures(dimensions: List[dict], table: str) -> List[Dict[str, str]]:
    """Generate measures from a list of dimensions.

    When no dimension-specific measures are found, return a single "count" measure.

    Raise ClickException if dimensions result in duplicate measures.
    """
    measures = {}
    for dimension in dimensions:
        dimension_name = dimension["name"]
        if dimension_name in {"client_id", "client_info__client_id"}:
            measure = {
                "name": "clients",
                "type": "count_distinct",
                "sql": f"${{{dimension_name}}}",
            }
        elif dimension_name == "document_id":
            measure = {"name": "ping_count", "type": "count"}
        else:
            continue
        name = measure["name"]
        if name in measures:
            raise click.ClickException(
                f"duplicate measure {name!r} for table {table!r}"
            )
        measures[name] = measure
    # return a generic count measure if no default measures were generated
    return list(measures.values()) or [{"name": "count", "type": "count"}]


def _generate_views(
    client, out_dir: Path, views: Dict[str, List[Dict[str, str]]]
) -> Iterable[Path]:
    for name, tables in views.items():
        view: Dict[str, Any] = {"name": name}
        # use schema for the table where channel=="release" or the first one
        table = next(
            (table for table in tables if table.get("channel") == "release"),
            tables[0],
        )["table"]
        # add dimensions and dimension groups
        dimensions = _generate_dimensions(client, table)
        view["dimensions"] = list(filterfalse(_is_dimension_group, dimensions))
        view["dimension_groups"] = list(filter(_is_dimension_group, dimensions))
        # add measures
        view["measures"] = _generate_measures(dimensions, table)
        if len(tables) > 1:
            # parameterize table name
            view["parameters"] = [
                {
                    "name": "channel",
                    "type": "unquoted",
                    "allowed_values": [
                        {
                            "label": table["channel"].title(),
                            "value": table["table"],
                        }
                        for table in tables
                    ],
                }
            ]
            view["sql_table_name"] = "`{% parameter channel %}`"
        else:
            view["sql_table_name"] = f"`{table}`"
        path = out_dir / f"{name}.view.lkml"
        path.write_text(lkml.dump({"views": [view]}))
        yield path


def _generate_explores(
    client, out_dir: Path, namespace: str, explores: dict
) -> Iterable[Path]:
    for explore_name, defn in explores.items():
        explore = explore_types[defn["type"]].from_dict(explore_name, defn)
        explore_lookml = explore.to_lookml()
        file_lookml = {
            "includes": f"/looker-hub/{namespace}/views/*.view.lkml",
            "explores": [explore_lookml],
        }
        path = out_dir / (explore_name + ".explore.lkml")
        path.write_text(lkml.dump(file_lookml))
        yield path


@click.command(help=__doc__)
@click.option(
    "--namespaces",
    default="namespaces.yaml",
    type=click.File(),
    help="Path to a yaml namespaces file",
)
@click.option(
    "--target-dir",
    default="looker-hub/",
    type=click.Path(),
    help="Path to a directory where lookml will be written",
)
def lookml(namespaces, target_dir):
    """Generate lookml from namespaces."""
    client = bigquery.Client()
    _namespaces = yaml.safe_load(namespaces)
    target = Path(target_dir)
    for namespace, value in _namespaces.items():
        logging.info(f"\nGenerating namespace {namespace}")

        view_dir = target / namespace / "views"
        view_dir.mkdir(parents=True, exist_ok=True)
        views = value.get("views", {})

        logging.info("  Generating views")
        for view_path in _generate_views(client, view_dir, views):
            logging.info(f"    ...Generating {view_path}")

        explore_dir = target / namespace / "explores"
        explore_dir.mkdir(parents=True, exist_ok=True)
        explores = value.get("explores", {})
        logging.info("  Generating explores")
        for explore_path in _generate_explores(
            client, explore_dir, namespace, explores
        ):
            logging.info(f"    ...Generating {explore_path}")
