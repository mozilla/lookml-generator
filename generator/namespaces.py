"""Generate namespaces.yaml."""
import gzip
import json
import tarfile
import urllib.request
import warnings
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from typing import Dict, List, Union

import click
import yaml

from .explores import explore_types
from .views import View, view_types

PROBE_INFO_BASE_URI = "https://probeinfo.telemetry.mozilla.org"


def _get_first(tuple_):
    return tuple_[0]


def _get_db_views(uri):
    with urllib.request.urlopen(uri) as f:
        tarbytes = BytesIO(f.read())
    views = defaultdict(dict)
    with tarfile.open(fileobj=tarbytes, mode="r:gz") as tar:
        for tarinfo in tar:
            if tarinfo.name.endswith("/metadata.yaml"):
                metadata = yaml.safe_load(tar.extractfile(tarinfo.name))
                references = metadata.get("references", {})
                if "view.sql" not in references:
                    continue
                *_, project, dataset_id, view_id, _ = tarinfo.name.split("/")
                if project == "moz-fx-data-shared-prod":
                    views[dataset_id][view_id] = [
                        ref.split(".") for ref in references["view.sql"]
                    ]
    return views


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
        app_listings = sorted(json.loads(gzip.decompress(f.read())), key=get_app_name)

    apps = []
    for app_name, group in groupby(app_listings, get_app_name):
        variants = list(group)

        # use canonical_app_name where channel=="release" or the first one
        canonical_app_name = next(
            (
                channel
                for channel in variants
                if channel.get("app_channel") == "release"
            ),
            variants[0],
        )["canonical_app_name"]

        channels = [
            {
                "channel": channel.get("app_channel"),
                "dataset": channel.get("bq_dataset_family"),
            }
            for channel in variants
            if not channel.get("deprecated")
        ]

        # If all channels are deprecated, don't include this app
        if channels:
            apps.append(
                {
                    "app_name": app_name,
                    "canonical_app_name": canonical_app_name,
                    "channels": channels,
                }
            )

    return apps


def _get_looker_views(
    app: Dict[str, Union[str, List[Dict[str, str]]]],
    db_views: Dict[str, Dict[str, List[List[str]]]],
) -> List[View]:
    views, view_names = [], []

    for klass in view_types.values():
        for view in klass.from_db_views(  # type: ignore
            app["app_name"], app["channels"], db_views
        ):
            if view.name in view_names:
                raise KeyError(
                    (
                        f"Duplicate Looker View name {view.name} "
                        f"when generating views for namespace {app['app_name']}"
                    )
                )
            views.append(view)
            view_names.append(view.name)

    return views


def _get_explores(views: List[View]) -> dict:
    explores = {}
    for _, klass in explore_types.items():
        for explore in klass.from_views(views):  # type: ignore
            explores.update(explore.to_dict())

    return explores


@click.command(help=__doc__)
@click.option(
    "--custom-namespaces",
    default=None,
    type=click.File(),
    help="Path to a custom namespaces file",
)
@click.option(
    "--generated-sql-uri",
    default="https://github.com/mozilla/bigquery-etl/archive/generated-sql.tar.gz",
    help="URI of a tar archive of the bigquery-etl generated-sql branch, which is "
    "used to list views and determine whether they reference stable tables",
)
@click.option(
    "--app-listings-uri",
    default="https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings",
    help="URI for probeinfo service v2 glean app listings",
)
def namespaces(custom_namespaces, generated_sql_uri, app_listings_uri):
    """Generate namespaces.yaml."""
    warnings.filterwarnings("ignore", module="google.auth._default")

    glean_apps = _get_glean_apps(app_listings_uri)
    db_views = _get_db_views(generated_sql_uri)

    namespaces = {}
    for app in glean_apps:
        looker_views = _get_looker_views(app, db_views)
        explores = _get_explores(looker_views)
        views_as_dict = {view.name: view.tables for view in looker_views}

        namespaces[app["app_name"]] = {
            "canonical_app_name": app["canonical_app_name"],
            "views": views_as_dict,
            "explores": explores,
        }

    if custom_namespaces is not None:
        namespaces.update(yaml.safe_load(custom_namespaces.read()))

    Path("namespaces.yaml").write_text(yaml.safe_dump(namespaces))
