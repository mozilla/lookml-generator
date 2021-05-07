"""Generate lookml from namespaces."""
import logging
from pathlib import Path
from typing import Dict, Iterable, Optional

import click
import yaml
from google.cloud import bigquery

from .explores import EXPLORE_TYPES
from .lkml_update import dump
from .namespaces import _get_glean_apps
from .views import VIEW_TYPES, View, ViewDict


def _generate_views(
    client, out_dir: Path, views: Iterable[View], v1_name: Optional[str]
) -> Iterable[Path]:
    for view in views:
        logging.info(
            f"Generating lookml for view {view.name} in {view.namespace} of type {view.view_type}"
        )
        path = out_dir / f"{view.name}.view.lkml"
        lookml = view.to_lookml(client, v1_name)
        path.write_text(dump(lookml))
        yield path


def _generate_explores(
    client, out_dir: Path, namespace: str, explores: dict, views_dir: Path
) -> Iterable[Path]:
    for explore_name, defn in explores.items():
        logging.info(f"Generating lookml for explore {explore_name} in {namespace}")
        explore = EXPLORE_TYPES[defn["type"]].from_dict(explore_name, defn, views_dir)
        file_lookml = {
            # Looker validates all included files,
            # so if we're not explicit about files here, validation takes
            # forever as looker re-validates all views for every explore (if we used *).
            "includes": [
                f"/looker-hub/{namespace}/views/{view}.view.lkml"
                for view in explore.get_dependent_views()
            ],
            "explores": [explore.to_lookml()],
        }
        path = out_dir / (explore_name + ".explore.lkml")
        path.write_text(dump(file_lookml))
        yield path


def _get_views_from_dict(views: Dict[str, ViewDict], namespace: str) -> Iterable[View]:
    for view_name, view_info in views.items():
        yield VIEW_TYPES[view_info["type"]].from_dict(  # type: ignore
            namespace, view_name, view_info
        )


def _glean_apps_to_v1_map(glean_apps):
    return {d["name"]: d["v1_name"] for d in glean_apps}


def _lookml(namespaces, glean_apps, target_dir):
    client = bigquery.Client()
    _namespaces = yaml.safe_load(namespaces)
    target = Path(target_dir)
    v1_mapping = _glean_apps_to_v1_map(glean_apps)
    for namespace, lookml_objects in _namespaces.items():
        logging.info(f"\nGenerating namespace {namespace}")

        view_dir = target / namespace / "views"
        view_dir.mkdir(parents=True, exist_ok=True)
        views = _get_views_from_dict(lookml_objects.get("views", {}), namespace)

        logging.info("  Generating views")
        v1_name: Optional[str] = v1_mapping.get(namespace)
        for view_path in _generate_views(client, view_dir, views, v1_name):
            logging.info(f"    ...Generating {view_path}")

        explore_dir = target / namespace / "explores"
        explore_dir.mkdir(parents=True, exist_ok=True)
        explores = lookml_objects.get("explores", {})
        logging.info("  Generating explores")
        for explore_path in _generate_explores(
            client, explore_dir, namespace, explores, view_dir
        ):
            logging.info(f"    ...Generating {explore_path}")


@click.command(help=__doc__)
@click.option(
    "--namespaces",
    default="namespaces.yaml",
    type=click.File(),
    help="Path to a yaml namespaces file",
)
@click.option(
    "--app-listings-uri",
    default="https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings",
    help="URI for probeinfo service v2 glean app listings",
)
@click.option(
    "--target-dir",
    default="looker-hub/",
    type=click.Path(),
    help="Path to a directory where lookml will be written",
)
def lookml(namespaces, app_listings_uri, target_dir):
    """Generate lookml from namespaces."""
    glean_apps = _get_glean_apps(app_listings_uri)
    return _lookml(namespaces, glean_apps, target_dir)
