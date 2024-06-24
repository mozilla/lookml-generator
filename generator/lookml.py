"""Generate lookml from namespaces."""

import logging
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import click
import lkml
import yaml
from google.cloud import bigquery

from .dashboards import DASHBOARD_TYPES
from .explores import EXPLORE_TYPES
from .metrics_utils import LOOKER_METRIC_HUB_REPO, METRIC_HUB_REPO, MetricsConfigLoader
from .namespaces import _get_glean_apps
from .views import VIEW_TYPES, View, ViewDict
from .views.datagroups import generate_datagroups

FILE_HEADER = """
# *Do not manually modify this file*
#
# This file has been generated via https://github.com/mozilla/lookml-generator
# You can extend this view in the looker-spoke-default project (https://github.com/mozilla/looker-spoke-default)

"""


def _generate_view(
    client, out_dir: Path, view: View, v1_name: Optional[str]
) -> Optional[Path]:
    logging.info(
        f"Generating lookml for view {view.name} in {view.namespace} of type {view.view_type}"
    )
    path = out_dir / f"{view.name}.view.lkml"
    lookml = view.to_lookml(client, v1_name)
    if lookml == {}:
        return None

    # lkml.dump may return None, in which case write an empty file
    path.write_text(FILE_HEADER + (lkml.dump(lookml) or ""))
    return path


def _generate_explore(
    client,
    out_dir: Path,
    namespace: str,
    explore_name: str,
    explore: Any,
    views_dir: Path,
    v1_name: Optional[
        str
    ],  # v1_name for Glean explores: see: https://mozilla.github.io/probe-scraper/#tag/library
) -> Path:
    logging.info(f"Generating lookml for explore {explore_name} in {namespace}")
    explore = EXPLORE_TYPES[explore["type"]].from_dict(explore_name, explore, views_dir)
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
    return path


def _generate_dashboard(
    client,
    dash_dir: Path,
    namespace: str,
    dashboard_name: str,
    dashboard: Any,
):
    logging.info(f"Generating lookml for dashboard {dashboard_name} in {namespace}")
    dashboard = DASHBOARD_TYPES[dashboard["type"]].from_dict(
        namespace, dashboard_name, dashboard
    )

    dashboard_lookml = dashboard.to_lookml(client)
    dash_path = dash_dir / f"{dashboard_name}.dashboard.lookml"
    dash_path.write_text(FILE_HEADER + dashboard_lookml)
    return dash_path


def _get_views_from_dict(views: Dict[str, ViewDict], namespace: str) -> Iterable[View]:
    for view_name, view_info in views.items():
        yield VIEW_TYPES[view_info["type"]].from_dict(  # type: ignore
            namespace, view_name, view_info
        )


def _glean_apps_to_v1_map(glean_apps):
    return {d["name"]: d["v1_name"] for d in glean_apps}


def _lookml(
    namespaces, glean_apps, target_dir, namespace_filter=[], parallelism: int = 8
):
    client = bigquery.Client()

    namespaces_content = namespaces.read()
    _namespaces = yaml.safe_load(namespaces_content)
    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)

    # Write namespaces file to target directory, for use
    # by the Glean Dictionary and other tools
    with open(target / "namespaces.yaml", "w") as target_namespaces_file:
        target_namespaces_file.write(namespaces_content)

    views_with_v1_name = []
    v1_mapping = _glean_apps_to_v1_map(glean_apps)
    for namespace, lookml_objects in _namespaces.items():
        if len(namespace_filter) == 0 or namespace in namespace_filter:
            logging.info(f"\nGenerating namespace {namespace}")

            view_dir = target / namespace / "views"
            view_dir.mkdir(parents=True, exist_ok=True)
            views = list(
                _get_views_from_dict(lookml_objects.get("views", {}), namespace)
            )

            logging.info("  Generating views")
            v1_name: Optional[str] = v1_mapping.get(namespace)
            for view in views:
                views_with_v1_name.append((view_dir, view, v1_name))

    with ThreadPool(parallelism) as pool:
        pool.starmap(partial(_generate_view, client), views_with_v1_name)

    explores_with_v1_name = []
    for namespace, lookml_objects in _namespaces.items():
        if len(namespace_filter) == 0 or namespace in namespace_filter:
            logging.info("  Generating datagroups")
            generate_datagroups(views, target, namespace, client)

            view_dir = target / namespace / "views"
            explore_dir = target / namespace / "explores"
            explore_dir.mkdir(parents=True, exist_ok=True)
            explores = lookml_objects.get("explores", {})
            logging.info("  Generating explores")
            explores_with_v1_name += [
                (explore_dir, namespace, explore_name, explore, view_dir, v1_name)
                for explore_name, explore in explores.items()
            ]

    with ThreadPool(parallelism) as pool:
        pool.starmap(partial(_generate_explore, client), explores_with_v1_name)

    dashboards_with_namespace = []
    for namespace, lookml_objects in _namespaces.items():
        if len(namespace_filter) == 0 or namespace in namespace_filter:
            logging.info("  Generating dashboards")
            dashboard_dir = target / namespace / "dashboards"
            dashboard_dir.mkdir(parents=True, exist_ok=True)
            dashboards = lookml_objects.get("dashboards", {})
            dashboards_with_namespace += [
                (dashboard_dir, namespace, dashboard_name, dashboard)
                for dashboard_name, dashboard in dashboards.items()
            ]

    with ThreadPool(parallelism) as pool:
        pool.starmap(
            partial(_generate_dashboard, client),
            dashboards_with_namespace,
        )


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
@click.option(
    "--metric-hub-repos",
    "--metric-hub-repos",
    multiple=True,
    default=[METRIC_HUB_REPO, LOOKER_METRIC_HUB_REPO],
    help="Repos to load metric configs from.",
)
@click.option(
    "--only",
    multiple=True,
    default=[],
    help="List of namespace names to generate lookml for.",
)
@click.option(
    "--parallelism",
    "-p",
    default=8,
    type=int,
    help="Number of threads to use for lookml generation",
)
def lookml(
    namespaces, app_listings_uri, target_dir, metric_hub_repos, only, parallelism
):
    """Generate lookml from namespaces."""
    if metric_hub_repos:
        MetricsConfigLoader.update_repos(metric_hub_repos)

    glean_apps = _get_glean_apps(app_listings_uri)
    return _lookml(namespaces, glean_apps, target_dir, only, parallelism)
