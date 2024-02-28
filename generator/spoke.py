"""Generate directories and models for new namespaces."""

import logging
import os
from pathlib import Path
from typing import Dict, List, TypedDict

import click
import lkml
import looker_sdk
import yaml

from .lookml import ViewDict

MODEL_SETS_BY_INSTANCE: Dict[str, List[str]] = {
    "https://mozilladev.cloud.looker.com": ["mozilla_confidential"],
    "https://mozillastaging.cloud.looker.com": ["mozilla_confidential"],
    "https://mozilla.cloud.looker.com": ["mozilla_confidential"],
}

DEFAULT_DB_CONNECTION = "telemetry"


class ExploreDict(TypedDict):
    """Represent an explore definition."""

    type: str
    views: List[Dict[str, str]]


class NamespaceDict(TypedDict):
    """Represent a Namespace definition."""

    views: ViewDict
    explores: ExploreDict
    pretty_name: str
    glean_app: bool
    connection: str
    spoke: str


def setup_env_with_looker_creds() -> bool:
    """
    Set up env with looker credentials.

    Returns TRUE if the config is complete.
    """
    client_id = os.environ.get("LOOKER_API_CLIENT_ID")
    client_secret = os.environ.get("LOOKER_API_CLIENT_SECRET")
    instance = os.environ.get("LOOKER_INSTANCE_URI")

    if client_id is None or client_secret is None or instance is None:
        return False

    os.environ["LOOKERSDK_BASE_URL"] = instance
    os.environ["LOOKERSDK_API_VERSION"] = "4.0"
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true"
    os.environ["LOOKERSDK_TIMEOUT"] = "120"
    os.environ["LOOKERSDK_CLIENT_ID"] = client_id
    os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret

    return True


def generate_model(
    spoke_path: Path, name: str, namespace_defn: NamespaceDict, db_connection: str
) -> Path:
    """
    Generate a model file for a namespace.

    We want these to have a nice label and a unique name.
    We only import explores and dashboards, as we want those
    to auto-import upon generation.

    Views are not imported by default, since they should
    be added one-by-one if they are included in an explore.
    """
    logging.info(f"Generating model {name}...")
    model_defn = {
        "connection": db_connection,
        "label": namespace_defn["pretty_name"],
    }

    # automatically import generated explores for new glean apps
    has_explores = len(namespace_defn.get("explores", {})) > 0

    path = spoke_path / name / f"{name}.model.lkml"
    # lkml.dump may return None, in which case write an empty file
    footer_text = f"""
# Include files from looker-hub or spoke-default below. For example:
{'' if has_explores else '# '}include: "//looker-hub/{name}/explores/*"
# include: "//looker-hub/{name}/dashboards/*"
# include: "//looker-hub/{name}/views/*"
# include: "views/*"
# include: "explores/*"
# include: "dashboards/*"
"""
    model_text = lkml.dump(model_defn)
    if model_text is None:
        path.write_text("")
    else:
        path.write_text(model_text + footer_text)

    return path


def configure_model(
    sdk: looker_sdk.methods40.Looker40SDK,
    model_name: str,
    db_connection: str,
    spoke_project: str,
):
    """Configure a Looker model by name."""
    instance = os.environ["LOOKER_INSTANCE_URI"]
    logging.info(f"Configuring model {model_name}...")

    try:
        sdk.lookml_model(model_name)
        logging.info("Model is configured!")
        return
    except looker_sdk.error.SDKError:
        pass

    sdk.create_lookml_model(
        looker_sdk.models40.WriteLookmlModel(
            allowed_db_connection_names=[db_connection],
            name=model_name,
            project_name=spoke_project,
        )
    )

    for model_set_name in MODEL_SETS_BY_INSTANCE[instance]:
        model_sets = sdk.search_model_sets(name=model_set_name)
        if len(model_sets) != 1:
            raise click.ClickException("Error: Found more than one matching model set")

        model_set = model_sets[0]
        models, _id = model_set.models, model_set.id
        if models is None or _id is None:
            raise click.ClickException("Error: Missing models or name from model_set")

        sdk.update_model_set(
            _id, looker_sdk.models40.WriteModelSet(models=list(models) + [model_name])
        )


def generate_directories(
    namespaces: Dict[str, NamespaceDict], base_dir: Path, sdk_setup=False
):
    """Generate directories and model for a namespace, if it doesn't exist."""
    for namespace, defn in namespaces.items():
        spoke = defn["spoke"]
        spoke_dir = base_dir / spoke
        spoke_dir.mkdir(parents=True, exist_ok=True)
        print(f"Writing {namespace} to {spoke_dir}")
        existing_dirs = {p.name for p in spoke_dir.iterdir()}

        if namespace in existing_dirs:
            continue

        (spoke_dir / namespace).mkdir()
        for dirname in ("views", "explores", "dashboards"):
            (spoke_dir / namespace / dirname).mkdir()
            (spoke_dir / namespace / dirname / ".gitkeep").touch()

        db_connection: str = defn.get("connection", DEFAULT_DB_CONNECTION)
        generate_model(spoke_dir, namespace, defn, db_connection)

        if sdk_setup:
            spoke_project = spoke.lstrip("looker-")
            sdk = looker_sdk.init40()
            logging.info("Looker SDK 4.0 initialized successfully.")
            configure_model(sdk, namespace, db_connection, spoke_project)


@click.command(help=__doc__)
@click.option(
    "--namespaces",
    default="namespaces.yaml",
    type=click.File(),
    help="Path to the namespaces.yaml file.",
)
@click.option(
    "--spoke-dir",
    default=".",
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    help="Directory containing the Looker spoke.",
)
def update_spoke(namespaces, spoke_dir):
    """Generate updates to spoke project."""
    _namespaces = yaml.safe_load(namespaces)
    sdk_setup = setup_env_with_looker_creds()
    generate_directories(_namespaces, Path(spoke_dir), sdk_setup)
