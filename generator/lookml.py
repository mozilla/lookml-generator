"""Generate lookml from namespaces."""
import json
import re
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from textwrap import indent
from typing import Dict, Iterator, List, Optional, Tuple

import click
import yaml
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
    ) -> Iterator["Dimension"]:
        """Generate dimensions from a bigquery schema."""
        for field in schema:
            if field.field_type == "RECORD" and not field.mode == "REPEATED":
                yield from klass.from_schema(field.fields, *prefix, field.name)
            else:
                yield klass((*prefix, field.name), field.field_type, field.mode)


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
        """Generate measures from a list of dimensions."""
        measures = []
        for dimension in dimensions:
            if dimension.name in {"client_id", "client_info__client_id"}:
                measures.append(
                    klass("clients", "count_distinct", f"${{{dimension.name}}}")
                )
            if dimension.name == "document_id":
                measures.append(klass("ping_count", "count"))
        # add a generic count measure if no default measures were generated
        if not measures:
            measures.append(klass("count", "count"))
        return measures


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
    for key, value in _namespaces.items():
        view_dir = target / key / "views"
        view_dir.mkdir(parents=True, exist_ok=True)
        for view, tables in value.get("views", {}).items():
            path = view_dir / (view + ".view.lkml")
            # only view one table, either channel="release" or the first one
            for item in tables:
                if item.get("channel") == "release":
                    table = item["table"]
                    break
            else:
                # default to the first table
                table = tables[0]["table"]
            dimensions = list(Dimension.from_schema(client.get_table(table).schema))
            measures = Measure.from_dimensions(dimensions)
            path.write_text(
                f"view: {view} {{\n  sql_table_name: `{table}` ;;"
                + "".join(
                    sorted(
                        indent(f"\n\n{field.to_lookml()}", " " * 2)
                        for field in dimensions + measures
                    )
                )
                + "\n}"
            )
