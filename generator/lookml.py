"""Generate lookml from namespaces."""
import json
import re
from dataclasses import dataclass
from pathlib import Path
from textwrap import indent
from typing import Dict, List, Tuple

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

MEASURE_DIMENSIONS = {
    ("client_id",): "clients",
    ("client_info", "client_id"): "clients",
}

HIDDEN_DIMENSIONS = {
    ("document_id",),
    *MEASURE_DIMENSIONS.keys(),
}

MAP_LAYER_NAMES = {
    ("country",): "countries",
    ("metadata", "geo", "country"): "countries",
}


@dataclass
class Dimension:
    """LookML View Dimension."""

    path: Tuple[str, ...]
    field_type: str
    mode: str

    def to_lookml(self) -> str:
        """Serialize dimension as lookml."""
        key, name = "dimension", "__".join(self.path)
        attributes: Dict[str, str] = {}
        attributes["sql"] = "${TABLE}." + ".".join(self.path) + " ;;"
        if self.mode == "REPEATED" or self.path in HIDDEN_DIMENSIONS:
            attributes["hidden"] = "yes"
        else:
            attributes["type"] = BIGQUERY_TYPE_TO_DIMENSION_TYPE[self.field_type]
            if attributes["type"] == "time":
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
                    unsuffixed = re.sub("_date$", "", self.path[-1])
                else:
                    unsuffixed = re.sub("_time(stamp)?$", "", self.path[-1])
                if unsuffixed != "parsed":
                    name = "__".join((*self.path[:-1], unsuffixed))
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
            f"{key}: {name} {{"
            + indent(
                "".join(
                    f"\n{attr}: {value}" for attr, value in sorted(attributes.items())
                ),
                " " * 2,
            )
            + "\n}"
        )


def _get_dimensions(schema: List[bigquery.SchemaField], *prefix: str):
    for field in schema:
        if field.field_type == "RECORD" and not field.mode == "REPEATED":
            yield from _get_dimensions(field.fields, *prefix, field.name)
        else:
            yield Dimension((*prefix, field.name), field.field_type, field.mode)


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
            dimensions = list(_get_dimensions(client.get_table(table).schema))
            view_attributes = [
                indent(f"\n\n{dimension.to_lookml()}", " " * 2)
                for dimension in dimensions
            ]
            for dimension in dimensions:
                if dimension.path in MEASURE_DIMENSIONS:
                    view_attributes.append(
                        "\n"
                        f"\n  measure: {MEASURE_DIMENSIONS[dimension.path]} {{"
                        f"\n    sql: COUNT(DISTINCT {'.'.join(dimension.path)}) ;;"
                        "\n    type: number"
                        "\n  }"
                    )
            view_attributes.append(
                "\n" "\n  measure: ping_count {" "\n    type: count" "\n  }"
            )
            path.write_text(
                f"view: {view} {{\n  sql_table_name: `{table}` ;;"
                + "".join(sorted(view_attributes))
                + "\n}"
            )
