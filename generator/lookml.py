"""Generate lookml from namespaces."""

import logging
from functools import partial
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import click
import lkml
import yaml

from generator.utils import get_file_from_looker_hub

from .dashboards import DASHBOARD_TYPES
from .dryrun import DryRunContext, DryRunError, Errors, credentials, id_token
from .explores import EXPLORE_TYPES
from .metrics_utils import LOOKER_METRIC_HUB_REPO, METRIC_HUB_REPO, MetricsConfigLoader
from .namespaces import _get_glean_apps
from .views import VIEW_TYPES, View, ViewDict
from .views.datagroups import generate_datagroup

FILE_HEADER = """
# *Do not manually modify this file*
#
# This file has been generated via https://github.com/mozilla/lookml-generator
# You can extend this view in the looker-spoke-default project (https://github.com/mozilla/looker-spoke-default)

"""


def _generate_view(
    out_dir: Path,
    view: View,
    v1_name: Optional[str],
    dryrun,
) -> Optional[Path]:
    logging.info(
        f"Generating lookml for view {view.name} in {view.namespace} of type {view.view_type}"
    )
    path = out_dir / f"{view.name}.view.lkml"

    try:
        lookml = view.to_lookml(v1_name, dryrun)
        if lookml == {}:
            return None

        # lkml.dump may return None, in which case write an empty file
        path.write_text(FILE_HEADER + (lkml.dump(lookml) or ""))
        return path
    except DryRunError as e:
        if e.error == Errors.PERMISSION_DENIED and e.use_cloud_function:
            print(
                f"Permission error dry running {view.name}. Copy existing {path} file from looker-hub."
            )
            try:
                get_file_from_looker_hub(path)
                return path
            except Exception as ex:
                print(f"Skip generating view for {path}: {ex}")
                return None
        else:
            raise


def _generate_explore(
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
    explore_by_type = EXPLORE_TYPES[explore["type"]].from_dict(
        explore_name, explore, views_dir
    )
    file_lookml = {
        # Looker validates all included files,
        # so if we're not explicit about files here, validation takes
        # forever as looker re-validates all views for every explore (if we used *).
        "includes": [
            f"/looker-hub/{namespace}/views/{view}.view.lkml"
            for view in explore_by_type.get_dependent_views()
        ],
        "explores": explore_by_type.to_lookml(v1_name),
    }
    path = out_dir / (explore_name + ".explore.lkml")
    # lkml.dump may return None, in which case write an empty file
    path.write_text(FILE_HEADER + (lkml.dump(file_lookml) or ""))
    return path


def _generate_dashboard(
    dash_dir: Path,
    namespace: str,
    dashboard_name: str,
    dashboard_info: Any,
):
    logging.info(f"Generating lookml for dashboard {dashboard_name} in {namespace}")
    dashboard = DASHBOARD_TYPES[dashboard_info["type"]].from_dict(
        namespace, dashboard_name, dashboard_info
    )

    dashboard_lookml = dashboard.to_lookml()
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


def _run_generation(func):
    """
    Run the partially applied generate function.

    For parallel execution.
    """
    return func()


def _update_metric_repos(metric_hub_repos):
    """Update metric hub repos when initializing the processes."""
    MetricsConfigLoader.update_repos(metric_hub_repos)


def _lookml(
    namespaces,
    glean_apps,
    target_dir,
    dryrun,
    namespace_filter=[],
    parallelism: int = 8,
    metric_hub_repos=[],
):
    namespaces_content = namespaces.read()
    _namespaces = yaml.safe_load(namespaces_content)
    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)

    # Write namespaces file to target directory, for use
    # by the Glean Dictionary and other tools
    with open(target / "namespaces.yaml", "w") as target_namespaces_file:
        target_namespaces_file.write(namespaces_content)

    generate_views = []
    generate_datagroups = []
    generate_explores = []
    generate_dashboards = []
    v1_mapping = _glean_apps_to_v1_map(glean_apps)

    for namespace, lookml_objects in _namespaces.items():
        if len(namespace_filter) == 0 or namespace in namespace_filter:
            view_dir = target / namespace / "views"
            view_dir.mkdir(parents=True, exist_ok=True)
            views = list(
                _get_views_from_dict(lookml_objects.get("views", {}), namespace)
            )

            v1_name: Optional[str] = v1_mapping.get(namespace)
            for view in views:
                generate_views.append(
                    partial(
                        _generate_view,
                        view_dir,
                        view,
                        v1_name,
                        dryrun,
                    )
                )
                generate_datagroups.append(
                    partial(
                        generate_datagroup,
                        view,
                        target,
                        namespace,
                        dryrun,
                    )
                )

            explore_dir = target / namespace / "explores"
            explore_dir.mkdir(parents=True, exist_ok=True)
            explores = lookml_objects.get("explores", {})
            generate_explores += [
                partial(
                    _generate_explore,
                    explore_dir,
                    namespace,
                    explore_name,
                    explore,
                    view_dir,
                    v1_name,
                )
                for explore_name, explore in explores.items()
            ]

            dashboard_dir = target / namespace / "dashboards"
            dashboard_dir.mkdir(parents=True, exist_ok=True)
            dashboards = lookml_objects.get("dashboards", {})
            generate_dashboards += [
                partial(
                    _generate_dashboard,
                    dashboard_dir,
                    namespace,
                    dashboard_name,
                    dashboard,
                )
                for dashboard_name, dashboard in dashboards.items()
            ]

    if parallelism == 1:
        # run without using multiprocessing
        # this is needed for the unit tests to work as mocks are not shared across processes
        logging.info("  Generating views")
        for generate_view_func in generate_views:
            generate_view_func()
        logging.info("  Generating datagroups")
        for generate_datagroup_func in generate_datagroups:
            generate_datagroup_func()
        logging.info("  Generating explores")
        for generate_explore_func in generate_explores:
            generate_explore_func()
        logging.info("  Generating dashboards")
        for generate_dashboard_func in generate_dashboards:
            generate_dashboard_func()
    else:
        with Pool(
            parallelism, initializer=partial(_update_metric_repos, metric_hub_repos)
        ) as pool:
            logging.info("  Generating views and datagroups")
            pool.map(_run_generation, generate_views + generate_datagroups)
            logging.info("  Generating explores")
            pool.map(
                _run_generation,
                generate_explores,
            )
            logging.info("  Generating dashboards")
            pool.map(
                _run_generation,
                generate_dashboards,
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
    "--use_cloud_function",
    "--use-cloud-function",
    help="Use the Cloud Function to run dry runs during LookML generation.",
    type=bool,
)
@click.option(
    "--parallelism",
    "-p",
    default=8,
    type=int,
    help="Number of threads to use for lookml generation",
)
def lookml(
    namespaces,
    app_listings_uri,
    target_dir,
    metric_hub_repos,
    only,
    use_cloud_function,
    parallelism,
):
    """Generate lookml from namespaces."""
    if metric_hub_repos:
        MetricsConfigLoader.update_repos(metric_hub_repos)
    glean_apps = _get_glean_apps(app_listings_uri)

    dry_run_id_token = None
    creds = None
    if use_cloud_function:
        dry_run_id_token = id_token()
    else:
        creds = credentials()

    dryrun = DryRunContext(
        use_cloud_function=use_cloud_function,
        id_token=dry_run_id_token,
        credentials=creds,
    )

    return _lookml(
        namespaces,
        glean_apps,
        target_dir,
        dryrun,
        only,
        parallelism,
        metric_hub_repos,
    )
