"""Generate lookml from namespaces."""
import json
import logging
import re
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from textwrap import indent
from typing import Dict, Iterable, List, Optional, Tuple

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


@dataclass(frozen=True)
class Dimension:
    """LookML View Dimension."""

    path: Tuple[str, ...]
    field_type: str
    mode: str

    @cached_property
    def hidden(self) -> bool:
        """Determine whether this dimension is hidden."""
        return self.mode == "REPEATED" or self.path in HIDDEN_DIMENSIONS

    @cached_property
    def type(self) -> str:
        """Determine the lookml type for this dimension."""
        return BIGQUERY_TYPE_TO_DIMENSION_TYPE[self.field_type]

    @cached_property
    def name(self) -> str:
        """Determine the dimension or dimension_group name.

        Remove _{type} suffix from the last path element for time dimension_group names.
        For example submission_date and submission_timestamp become submission, and
        metadata.header.parsed_date becomes metadata__header__parsed. This is because
        the timeframe will add a {type} suffix to the individual dimension names.
        """
        if not self.hidden and self.type == "time":
            path = *self.path[:-1], re.sub("_(date|time(stamp)?)$", "", self.path[-1])
        else:
            path = self.path
        return "__".join(path)

    def to_lookml(self) -> str:
        """Serialize dimension as lookml."""
        key = "dimension"
        attributes: Dict[str, str] = {}
        attributes["sql"] = "${TABLE}." + ".".join(self.path) + " ;;"
        if self.hidden:
            attributes["hidden"] = "yes"
        else:
            attributes["type"] = self.type
            if self.type == "time":
                key = "dimension_group"
                timeframes = [
                    "raw",
                    "time",
                    "date",
                    "week",
                    "month",
                    "quarter",
                    "year",
                ]
                if self.field_type == "DATE":
                    timeframes.remove("time")
                    attributes["convert_tz"] = "no"
                    attributes["datatype"] = "date"
                attributes["timeframes"] = (
                    "[\n" + indent(",\n".join(timeframes), " " * 2) + "\n]"
                )
            if len(self.path) > 1:
                attributes["group_label"] = json.dumps(
                    " ".join(self.path[:-1]).replace("_", " ").title()
                )
                attributes["group_item_label"] = json.dumps(
                    self.path[-1].replace("_", " ").title()
                )
            if self.path in MAP_LAYER_NAMES:
                attributes["map_layer_name"] = MAP_LAYER_NAMES[self.path]
        return (
            f"{key}: {self.name} {{"
            + indent(
                "".join(
                    f"\n{attr}: {value}" for attr, value in sorted(attributes.items())
                ),
                " " * 2,
            )
            + "\n}"
        )

    @classmethod
    def from_schema(
        klass, schema: List[bigquery.SchemaField], *prefix: str
    ) -> List["Dimension"]:
        """Generate dimensions from a bigquery schema.

        When schema contains both submission_timestamp and submission_date, only produce
        a dimension group for submission_timestamp.

        Raise KeyError if schema results in duplicate dimensions.
        """
        dimensions = {}
        for field in sorted(schema, key=lambda f: f.name):
            if field.field_type == "RECORD" and not field.mode == "REPEATED":
                new_dimensions = klass.from_schema(field.fields, *prefix, field.name)
            else:
                path = *prefix, field.name
                new_dimensions = [klass(path, field.field_type, field.mode)]
            for dimension in new_dimensions:
                # overwrite duplicate "submission" dimension_group, thus picking the
                # last value sorted by field name, which is submission_timestamp
                if dimension.name in dimensions and dimension.name != "submission":
                    raise KeyError(dimension.name)
                dimensions[dimension.name] = dimension
        return list(dimensions.values())


@dataclass
class Measure:
    """LookML View Measure."""

    name: str
    type: str
    sql: Optional[str] = None

    def to_lookml(self) -> str:
        """Serialize measure as lookml."""
        return (
            f"measure: {self.name} {{"
            + f"\n  type: {self.type}"
            + (f"\n  sql: {self.sql} ;;" if self.sql is not None else "")
            + "\n}"
        )

    @classmethod
    def from_dimensions(klass, dimensions: List[Dimension]) -> List["Measure"]:
        """Generate measures from a list of dimensions.

        When no dimension-specific measures are found, return a single "count" measure.

        Raise KeyError if dimensions result in duplicate measures.
        """
        measures = {}
        for dimension in dimensions:
            if dimension.name in {"client_id", "client_info__client_id"}:
                measure = klass("clients", "count_distinct", f"${{{dimension.name}}}")
            elif dimension.name == "document_id":
                measure = klass("ping_count", "count")
            else:
                continue
            if measure.name in measures:
                raise KeyError(measure.name)
            measures[measure.name] = measure
        # return a generic count measure if no default measures were generated
        return list(measures.values()) or [klass("count", "count")]


def _generate_views(
    client, out_dir: Path, views: Dict[str, List[Dict[str, str]]]
) -> Iterable[Path]:
    for view, tables in views.items():
        # use schema for the table where channel=="release" or the first one
        table = next(
            (table for table in tables if table.get("channel") == "release"),
            tables[0],
        )["table"]
        try:
            dimensions = Dimension.from_schema(client.get_table(table).schema)
        except KeyError as e:
            raise click.ClickException(f"duplicate dimension {e} for table {table!r}")
        try:
            measures = Measure.from_dimensions(dimensions)
        except KeyError as e:
            raise click.ClickException(f"duplicate measure {e} for table {table!r}")
        fields = []
        sql_table_name = table
        if len(tables) > 1:
            # parameterize table name
            fields.append(
                "\n  parameter: channel {"
                "\n    type: unquoted"
                + "".join(
                    "\n    allowed_value: {"
                    f'\n      label: {json.dumps(table["channel"].title())}'
                    f'\n      value: {json.dumps(table["table"])}'
                    "\n    }"
                    for table in tables
                )
                + "\n  }"
            )
            sql_table_name = "{% parameter channel %}"
        fields.append(f"\n  sql_table_name: `{sql_table_name}` ;;")
        # add dimensions and measures after sql_table_name
        fields += sorted(
            indent(f"\n{field.to_lookml()}", " " * 2)
            for field in dimensions + measures  # type: ignore
        )
        path = out_dir / (view + ".view.lkml")
        path.write_text(f"view: {view} {{" + "\n".join(fields) + "\n}")

        yield path


def _generate_explores(
    client, out_dir: Path, namespace: str, explores: dict
) -> Iterable[Path]:
    for explore_name, defn in explores.items():
        explore = explore_types[defn["type"]].from_dict(explore_name, defn)
        explore_lookml = explore.to_lookml()
        file_lookml = {
            "includes": [f"/looker-hub/{namespace}/views/*.view.lkml"],
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
        logging.info(f"Generating namespace {namespace}")
        model_dir = target / namespace
        model_dir.mkdir(parents=True, exist_ok=True)
        model_path = model_dir / f"{namespace}.model.lkml"
        logging.info(f"  ...Generating {model_path}")
        model_path.write_text(
            lkml.dump(
                {
                    "connection": "telemetry",
                    "label": value["canonical_app_name"],
                    "includes": ["views/*.view", "explores/*.explore"],
                }
            )
        )

        view_dir = model_dir / "views"
        view_dir.mkdir(parents=True, exist_ok=True)
        views = value.get("views", {})

        logging.info("  Generating views")
        for view_path in _generate_views(client, view_dir, views):
            logging.info(f"    ...Generating {view_path}")

        explore_dir = model_dir / "explores"
        explore_dir.mkdir(parents=True, exist_ok=True)
        explores = value.get("explores", {})
        logging.info("  Generating explores")
        for explore_path in _generate_explores(
            client, explore_dir, namespace, explores
        ):
            logging.info(f"    ...Generating {explore_path}")
