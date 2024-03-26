"""Generate namespaces.yaml."""

import fnmatch
import json
import re
import urllib.request
import warnings
from collections.abc import Mapping
from datetime import datetime
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from typing import Any, Dict, List, Union

import click
import yaml
from google.cloud import bigquery

from generator import operational_monitoring_utils

from .explores import EXPLORE_TYPES
from .metrics_utils import LOOKER_METRIC_HUB_REPO, METRIC_HUB_REPO, MetricsConfigLoader
from .views import VIEW_TYPES, View, lookml_utils

DEFAULT_GENERATED_SQL_URI = (
    "https://github.com/mozilla/bigquery-etl/archive/generated-sql.tar.gz"
)

PROBE_INFO_BASE_URI = "https://probeinfo.telemetry.mozilla.org"
DEFAULT_SPOKE = "looker-spoke-default"
OPMON_DATASET = "operational_monitoring"
PROD_PROJECT = "moz-fx-data-shared-prod"


def _normalize_slug(name):
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _merge_namespaces(dct, merge_dct):
    """Recursively merge namespaces."""
    for k, _ in merge_dct.items():
        if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], Mapping):
            if "glean_app" in merge_dct[k] and merge_dct[k]["glean_app"] is False:
                # if glean_app gets set to False, Glean views and explores should not be generated
                dct[k] = merge_dct[k]
            else:
                _merge_namespaces(dct[k], merge_dct[k])
        else:
            if k == "owners" and "owners" in dct:
                # combine owners
                dct[k] += merge_dct[k]
            else:
                dct[k] = merge_dct[k]


def _get_opmon(bq_client: bigquery.Client, namespaces: Dict[str, Any]):
    om_content: Dict[str, Any] = {"views": {}, "explores": {}, "dashboards": {}}
    # get operational monitoring namespace information

    opmon_namespace = namespaces["operational_monitoring"]
    views = opmon_namespace.get("views")

    if views is None:
        print("No views defined for operational monitoring")
        return {}

    projects_view = views.get("projects")

    if projects_view is None:
        print("No projects view defined for operational monitoring")
        return {}

    projects_table = projects_view["tables"][0]["table"]
    projects = operational_monitoring_utils.get_active_projects(
        bq_client, project_table=projects_table
    )

    # Iterating over all defined operational monitoring projects
    for project in projects:
        table_prefix = _normalize_slug(project["slug"])
        project_name = lookml_utils.slug_to_title(
            re.sub("[^0-9a-zA-Z_]+", "_", "_".join(project["name"].lower().split(" ")))
        )
        branches = project.get("branches", ["enabled", "disabled"])

        # append view and explore for data type
        table = f"{PROD_PROJECT}.{OPMON_DATASET}.{table_prefix}_statistics"
        dimensions = operational_monitoring_utils.get_dimension_defaults(
            bq_client, table, project["dimensions"]
        )
        om_content["views"][table_prefix] = {
            "type": "operational_monitoring_view",
            "tables": [
                {
                    "table": table,
                    "xaxis": project["xaxis"],
                    "dimensions": dimensions,
                }
            ],
        }
        om_content["explores"][table_prefix] = {
            "type": "operational_monitoring_explore",
            "views": {"base_view": f"{table_prefix}"},
            "branches": branches,
            "xaxis": project["xaxis"],
            "dimensions": dimensions,
            "summaries": project["summaries"],
        }

        if "alerting" in project and project["alerting"]:
            # create an alerting view if available
            om_content["views"][f"{table_prefix}_alerts"] = {
                "type": "operational_monitoring_alerting_view",
                "tables": [
                    {
                        "table": f"{PROD_PROJECT}.{OPMON_DATASET}.{table_prefix}_alerts",
                    }
                ],
            }
            om_content["explores"][f"{table_prefix}_alerts"] = {
                "type": "operational_monitoring_alerting_explore",
                "views": {"base_view": f"{table_prefix}_alerts"},
            }

        om_content["dashboards"][table_prefix] = {
            "type": "operational_monitoring_dashboard",
            "title": project_name,
            "tables": [
                {
                    "explore": f"{table_prefix}",
                    "table": f"{PROD_PROJECT}.{OPMON_DATASET}.{table_prefix}_statistics",
                    "branches": branches,
                    "xaxis": project["xaxis"],
                    "compact_visualization": project.get(
                        "compact_visualization", False
                    ),
                    "dimensions": dimensions,
                    "group_by_dimension": project.get("group_by_dimension", None),
                    "summaries": project["summaries"],
                }
            ],
        }

        if "alerting" in project and project["alerting"]:
            om_content["dashboards"][table_prefix]["tables"].append(
                {
                    "explore": f"{table_prefix}_alerts",
                    "table": f"{PROD_PROJECT}.{OPMON_DATASET}.{table_prefix}_alerts",
                }
            )

    return om_content


def _get_metric_hub_namespaces(existing_namespaces):
    metric_hub_data_sources = _get_metric_hub_data_sources()

    metric_hub_namespaces = {}
    for namespace, metric_hub_data_sources in metric_hub_data_sources.items():
        # each data source definition is represented by a view and an explore
        explores = {}
        views = {}
        for data_source in sorted(metric_hub_data_sources):
            views[f"metric_definitions_{data_source}"] = {
                "type": "metric_definitions_view"
            }

            if (
                namespace in existing_namespaces
                and "client_counts" in existing_namespaces[namespace]["views"]
            ):
                views[f"metric_definitions_{data_source}"]["tables"] = [
                    {
                        "table": existing_namespaces[namespace]["views"][
                            "client_counts"
                        ]["tables"][0]["table"]
                    }
                ]
            elif (
                namespace in existing_namespaces
                and "baseline_clients_daily" in existing_namespaces[namespace]["views"]
            ):
                views[f"metric_definitions_{data_source}"]["tables"] = [
                    {
                        "table": existing_namespaces[namespace]["views"][
                            "baseline_clients_daily"
                        ]["tables"][0]["table"]
                    }
                ]

            explores[f"metric_definitions_{data_source}"] = {
                "type": "metric_definitions_explore",
                "views": {"base_view": f"metric_definitions_{data_source}"},
            }

        metric_hub_namespaces[namespace] = {
            "pretty_name": lookml_utils.slug_to_title(namespace),
            "views": views,
            "explores": explores,
        }

    return metric_hub_namespaces


def _get_glean_apps(
    app_listings_uri: str,
) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
    # define key function and reuse it for sorted and groupby
    if app_listings_uri.startswith(PROBE_INFO_BASE_URI):
        # For probe-info-service requests, add query param to bypass cloudfront cache
        app_listings_uri += f"?t={datetime.utcnow().isoformat()}"

    get_app_name = itemgetter("app_name")
    with urllib.request.urlopen(app_listings_uri) as f:
        # groupby requires input be sorted by key to produce one result per key
        app_listings = sorted(json.loads(f.read()), key=get_app_name)

    apps = []
    for app_name, group in groupby(app_listings, get_app_name):
        variants = list(group)

        # use canonical_app_name where channel=="release" or the first one
        release_variant = next(
            (
                channel
                for channel in variants
                if channel.get("app_channel") == "release"
            ),
            variants[0],
        )

        canonical_app_name = release_variant["canonical_app_name"]
        v1_name = release_variant["v1_name"]
        emails = release_variant["notification_emails"]

        # we use the `source_dataset` concept to figure out what reference
        # we should be looking for inside bigquery-etl
        # For release we are currently using an app-level dataset which
        # references the app id specific one (so we look for that view as
        # a reference).
        # For other channels, we refer to the stable tables
        channels = [
            {
                "channel": channel.get("app_channel"),
                "dataset": (
                    channel.get("app_name").replace("-", "_")
                    if channel.get("app_channel") == "release"
                    else channel.get("bq_dataset_family")
                ),
                "source_dataset": (
                    channel.get("bq_dataset_family")
                    if channel.get("app_channel") == "release"
                    else channel.get("bq_dataset_family") + "_stable"
                ),
            }
            for channel in variants
            if not channel.get("deprecated")
        ]

        # If all channels are deprecated, don't include this app
        if channels:
            apps.append(
                {
                    "name": app_name,
                    "pretty_name": canonical_app_name,
                    "channels": channels,
                    "owners": emails,
                    "glean_app": True,
                    "v1_name": v1_name,
                }
            )

    return apps


def _get_looker_views(
    app: Dict[str, Union[str, List[Dict[str, str]]]],
    db_views: Dict[str, Dict[str, List[List[str]]]],
) -> List[View]:
    views, view_names = [], []

    for klass in VIEW_TYPES.values():
        for view in klass.from_db_views(  # type: ignore
            app["name"], app["glean_app"], app["channels"], db_views
        ):
            if view.name in view_names:
                raise KeyError(
                    (
                        f"Duplicate Looker View name {view.name} "
                        f"when generating views for namespace {app['name']}"
                    )
                )
            views.append(view)
            view_names.append(view.name)

    return views


def _get_explores(views: List[View]) -> dict:
    explores = {}
    for _, klass in EXPLORE_TYPES.items():
        for explore in klass.from_views(views):  # type: ignore
            explores.update(explore.to_dict())

    return explores


def _get_metric_hub_data_sources() -> Dict[str, List[str]]:
    """Get data source definitions from metric-hub repository for each namespace."""
    data_sources_per_namespace: Dict[str, List[str]] = {}
    for definition in MetricsConfigLoader.configs.definitions:
        for data_source_slug in definition.spec.data_sources.definitions.keys():
            if (
                len(
                    MetricsConfigLoader.metrics_of_data_source(
                        data_source_slug, definition.platform
                    )
                )
                > 0  # ignore data sources that are not used for any metric definition
            ):
                if definition.platform in data_sources_per_namespace:
                    data_sources_per_namespace[definition.platform].append(
                        data_source_slug
                    )
                else:
                    data_sources_per_namespace[definition.platform] = [data_source_slug]

    return data_sources_per_namespace


@click.command(help=__doc__)
@click.option(
    "--custom-namespaces",
    default="custom-namespaces.yaml",
    type=click.File(),
    help="Path to a custom namespaces file",
)
@click.option(
    "--generated-sql-uri",
    default=DEFAULT_GENERATED_SQL_URI,
    help="URI of a tar archive of the bigquery-etl generated-sql branch, which is "
    "used to list views and determine whether they reference stable tables",
)
@click.option(
    "--app-listings-uri",
    default="https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings",
    help="URI for probeinfo service v2 glean app listings",
)
@click.option(
    "--disallowlist",
    type=click.File(),
    default="namespaces-disallowlist.yaml",
    help="Path to namespace disallow list",
)
@click.option(
    "--metric-hub-repos",
    "--metric_hub_repos",
    multiple=True,
    default=[METRIC_HUB_REPO, LOOKER_METRIC_HUB_REPO],
    help="Repos to load metric configs from.",
)
def namespaces(
    custom_namespaces,
    generated_sql_uri,
    app_listings_uri,
    disallowlist,
    metric_hub_repos,
):
    """Generate namespaces.yaml."""
    warnings.filterwarnings("ignore", module="google.auth._default")
    glean_apps = _get_glean_apps(app_listings_uri)
    db_views = lookml_utils.get_bigquery_view_reference_map(generated_sql_uri)

    namespaces = {}
    for app in glean_apps:
        looker_views = _get_looker_views(app, db_views)
        explores = _get_explores(looker_views)
        views_as_dict = {view.name: view.as_dict() for view in looker_views}

        namespaces[app["name"]] = {
            "owners": app["owners"],
            "pretty_name": app["pretty_name"],
            "views": views_as_dict,
            "explores": explores,
            "glean_app": True,
        }

    if custom_namespaces is not None:
        custom_namespaces = yaml.safe_load(custom_namespaces.read()) or {}

        # generating operational monitoring namespace, if available
        if "operational_monitoring" in custom_namespaces:
            client = bigquery.Client()
            opmon = _get_opmon(bq_client=client, namespaces=custom_namespaces)
            custom_namespaces["operational_monitoring"].update(opmon)

        _merge_namespaces(namespaces, custom_namespaces)

    if metric_hub_repos:
        MetricsConfigLoader.update_repos(metric_hub_repos)

    _merge_namespaces(namespaces, _get_metric_hub_namespaces(namespaces))

    disallowed_namespaces = yaml.safe_load(disallowlist.read()) or {}
    disallowed_regex = [
        fnmatch.translate(namespace) for namespace in disallowed_namespaces
    ]
    disallowed_namespaces_pattern = re.compile("|".join(disallowed_regex))

    updated_namespaces = {}
    for namespace, _ in namespaces.items():
        if not disallowed_namespaces_pattern.fullmatch(namespace):
            if "spoke" not in namespaces[namespace]:
                namespaces[namespace]["spoke"] = DEFAULT_SPOKE
            if "glean_app" not in namespaces[namespace]:
                namespaces[namespace]["glean_app"] = False
            updated_namespaces[namespace] = namespaces[namespace]

    Path("namespaces.yaml").write_text(yaml.safe_dump(updated_namespaces))
