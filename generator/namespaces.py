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

from .explores import EXPLORE_TYPES
from .views import VIEW_TYPES, View

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
        release_variant = next(
            (
                channel
                for channel in variants
                if channel.get("app_channel") == "release"
            ),
            variants[0],
        )

        canonical_app_name = release_variant["canonical_app_name"]
        emails = release_variant["notification_emails"]

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
                    "name": app_name,
                    "pretty_name": canonical_app_name,
                    "channels": channels,
                    "owners": emails,
                    "glean_app": True,
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
@click.option(
    "--allowlist",
    type=click.File(),
    default="namespace_allowlist.yaml",
    help="Path to namespace allow list",
)
def namespaces(custom_namespaces, generated_sql_uri, app_listings_uri, allowlist):
    """Generate namespaces.yaml."""
    warnings.filterwarnings("ignore", module="google.auth._default")
    glean_apps = _get_glean_apps(app_listings_uri)
    db_views = _get_db_views(generated_sql_uri)

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
        namespaces.update(yaml.safe_load(custom_namespaces.read()) or {})

    allowed_namespaces = yaml.safe_load(allowlist.read())
    namespaces = {
        name: defn for name, defn in namespaces.items() if name in allowed_namespaces
    }

    Path("namespaces.yaml").write_text(yaml.safe_dump(namespaces))
