"""Generate namespaces.yaml."""
import tarfile
import urllib
import warnings
from io import BytesIO
from itertools import groupby
from operator import itemgetter
from pathlib import Path

import click
import requests
import yaml


def _get_first(tuple_):
    return tuple_[0]


def _get_views(uri):
    with urllib.request.urlopen(uri) as f:
        tarbytes = BytesIO(f.read())
    views = {}
    with tarfile.open(fileobj=tarbytes, mode="r:gz") as tar:
        for tarinfo in tar:
            if tarinfo.name.endswith("/view.sql"):
                *_, project, dataset_id, view_id, _ = tarinfo.name.split("/")
                if project == "moz-fx-data-shared-prod":
                    if dataset_id not in views:
                        views[dataset_id] = {}
                    views[dataset_id][view_id] = tar.extractfile(tarinfo.name)
    return views


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
def namespaces(custom_namespaces, generated_sql_uri):
    """Generate namespaces.yaml."""
    warnings.filterwarnings("ignore", module="google.auth._default")

    get_app_name = itemgetter("app_name")
    app_listings = sorted(
        requests.get(
            "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"
        ).json(),
        key=get_app_name,
    )
    app_filters = {"app_channel": "channel", "app_id": "app_id"}
    view_definitions = _get_views(generated_sql_uri)
    namespaces = {}
    for app_name, group in groupby(app_listings, get_app_name):
        apps = list(group)
        views = {}
        canonical_app_name = None
        for app in apps:
            if canonical_app_name is None or app.get("app_channel") == "release":
                canonical_app_name = app["canonical_app_name"]
            dataset_id = app["bq_dataset_family"]
            for view_id in view_definitions[dataset_id]:
                if view_id == "deletion_request":
                    continue
                if view_id not in views:
                    views[view_id] = []
                table = {"table": f"mozdata.{dataset_id}.{view_id}"}
                for attr, column in app_filters.items():
                    if attr in app:
                        table[column] = app[attr]
                views[view_id].append(table)

        namespaces[app_name] = {
            "canonical_app_name": canonical_app_name,
            "views": views,
        }

    if custom_namespaces is not None:
        namespaces.update(yaml.safe_load(custom_namespaces.read()))

    (Path(__file__).parent.parent / "namespaces.yaml").write_text(yaml.dump(namespaces))
