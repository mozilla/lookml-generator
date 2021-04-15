"""Generate lookml from namespaces."""
import logging
from pathlib import Path
from typing import Dict, Iterable

import click
import lkml
import yaml
from google.cloud import bigquery

from .explores import explore_types
from .views import GrowthAccountingView, View, ViewDict, view_types


def _generate_views(client, out_dir: Path, views: Iterable[View]) -> Iterable[Path]:
    for view in views:
        if view.view_type == GrowthAccountingView.type:
            continue

        path = out_dir / f"{view.name}.view.lkml"
        lookml = {"views": view.to_lookml(client)}
        path.write_text(lkml.dump(lookml))
        yield path


def _generate_explores(
    client, out_dir: Path, namespace: str, explores: dict
) -> Iterable[Path]:
    for explore_name, defn in explores.items():
        if defn["type"] != "ping_explore":
            continue

        explore = explore_types[defn["type"]].from_dict(explore_name, defn)
        file_lookml = {
            "includes": f"/looker-hub/{namespace}/views/*.view.lkml",
            "explores": [explore.to_lookml()],
        }
        path = out_dir / (explore_name + ".explore.lkml")
        path.write_text(lkml.dump(file_lookml))
        yield path


def _get_views_from_dict(views: Dict[str, ViewDict]) -> Iterable[View]:
    for view_name, view_info in views.items():
        yield view_types[view_info["type"]].from_dict(view_name, view_info)  # type: ignore


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
    for namespace, lookml_objects in _namespaces.items():
        logging.info(f"\nGenerating namespace {namespace}")

        view_dir = target / namespace / "views"
        view_dir.mkdir(parents=True, exist_ok=True)
        views = _get_views_from_dict(lookml_objects.get("views", {}))

        logging.info("  Generating views")
        for view_path in _generate_views(client, view_dir, views):
            logging.info(f"    ...Generating {view_path}")

        explore_dir = target / namespace / "explores"
        explore_dir.mkdir(parents=True, exist_ok=True)
        explores = lookml_objects.get("explores", {})
        logging.info("  Generating explores")
        for explore_path in _generate_explores(
            client, explore_dir, namespace, explores
        ):
            logging.info(f"    ...Generating {explore_path}")
