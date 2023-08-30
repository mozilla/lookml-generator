"""Generate lookml from namespaces."""
import logging
from pathlib import Path
from typing import Dict, Iterable, Optional

import click
import lkml
import yaml
from google.cloud import bigquery

from .dashboards import DASHBOARD_TYPES
from .explores import EXPLORE_TYPES
from .namespaces import _get_glean_apps
from .views import VIEW_TYPES, View, ViewDict
from .views.datagroups import generate_datagroups

FILE_HEADER = """
# *Do not manually modify this file*
#
# This file has been generated via https://github.com/mozilla/lookml-generator
# You can extend this view in the looker-spoke-default project (https://github.com/mozilla/looker-spoke-default)

"""


def _generate_views(
    client, out_dir: Path, views: Iterable[View], v1_name: Optional[str]
) -> Iterable[Path]:
    for view in views:
        logging.info(
            f"Generating lookml for view {view.name} in {view.namespace} of type {view.view_type}"
        )
        path = out_dir / f"{view.name}.view.lkml"
        lookml = view.to_lookml(client, v1_name)
        if lookml == {}:
            continue

        # lkml.dump may return None, in which case write an empty file
        path.write_text(FILE_HEADER + (lkml.dump(lookml) or ""))
        yield path


def _generate_explores(
    client,
    out_dir: Path,
    namespace: str,
    explores: dict,
    views_dir: Path,
    v1_name: Optional[
        str
    ],  # v1_name for Glean explores: see: https://mozilla.github.io/probe-scraper/#tag/library
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
            "explores": explore.to_lookml(client, v1_name),
        }
        path = out_dir / (explore_name + ".explore.lkml")
        # lkml.dump may return None, in which case write an empty file
        path.write_text(FILE_HEADER + (lkml.dump(file_lookml) or ""))
        yield path


def _generate_dashboards(
    client,
    dash_dir: Path,
    namespace: str,
    dashboards: dict,
):
    for dashboard_name, dashboard_info in dashboards.items():
        logging.info(f"Generating lookml for dashboard {dashboard_name} in {namespace}")
        dashboard = DASHBOARD_TYPES[dashboard_info["type"]].from_dict(
            namespace, dashboard_name, dashboard_info
        )

        dashboard_lookml = dashboard.to_lookml(client)
        dash_path = dash_dir / f"{dashboard_name}.dashboard.lookml"
        dash_path.write_text(FILE_HEADER + dashboard_lookml)
        yield dash_path


def _get_views_from_dict(views: Dict[str, ViewDict], namespace: str) -> Iterable[View]:
    for view_name, view_info in views.items():
        yield VIEW_TYPES[view_info["type"]].from_dict(  # type: ignore
            namespace, view_name, view_info
        )


def _glean_apps_to_v1_map(glean_apps):
    return {d["name"]: d["v1_name"] for d in glean_apps}


def _lookml(namespaces, glean_apps, target_dir):
    client = bigquery.Client()

    namespaces_content = namespaces.read()
    _namespaces = yaml.safe_load(namespaces_content)
    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)

    # Write namespaces file to target directory, for use
    # by the Glean Dictionary and other tools
    with open(target / "namespaces.yaml", "w") as target_namespaces_file:
        target_namespaces_file.write(namespaces_content)

    v1_mapping = _glean_apps_to_v1_map(glean_apps)
    for namespace, lookml_objects in _namespaces.items():
        logging.info(f"\nGenerating namespace {namespace}")

        view_dir = target / namespace / "views"
        view_dir.mkdir(parents=True, exist_ok=True)
        views = list(_get_views_from_dict(lookml_objects.get("views", {}), namespace))

        logging.info("  Generating views")
        v1_name: Optional[str] = v1_mapping.get(namespace)
        for view_path in _generate_views(client, view_dir, views, v1_name):
            logging.info(f"    ...Generating {view_path}")

        logging.info("  Generating datagroups")
        generate_datagroups(views, target, namespace, client)

        explore_dir = target / namespace / "explores"
        explore_dir.mkdir(parents=True, exist_ok=True)
        explores = lookml_objects.get("explores", {})
        logging.info("  Generating explores")
        for explore_path in _generate_explores(
            client, explore_dir, namespace, explores, view_dir, v1_name
        ):
            logging.info(f"    ...Generating {explore_path}")

        logging.info("  Generating dashboards")
        dashboard_dir = target / namespace / "dashboards"
        dashboard_dir.mkdir(parents=True, exist_ok=True)
        dashboards = lookml_objects.get("dashboards", {})
        for dashboard_path in _generate_dashboards(
            client, dashboard_dir, namespace, dashboards
        ):
            logging.info(f"    ...Generating {dashboard_path}")


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
