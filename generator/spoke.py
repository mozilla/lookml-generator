"""Generate directories and models for new namespaces."""

import logging
import os
from pathlib import Path
from typing import Dict, List, TypedDict

import click
import lkml
import looker_sdk
import yaml

from .content import setup_env_with_looker_creds
from .lookml import ViewDict

MODEL_SETS_BY_INSTANCE: Dict[str, List[str]] = {
    "https://mozilladev.cloud.looker.com": ["mozilla_confidential"],
    "https://mozillastaging.cloud.looker.com": ["mozilla_confidential"],
    "https://mozilla.cloud.looker.com": ["user-spokes", "spokes"],
}


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


def generate_model(spoke_path: Path, name: str, namespace_defn: NamespaceDict) -> Path:
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
        "connection": "telemetry",
        "label": namespace_defn["pretty_name"],
        "includes": [
            f"//looker-hub/{name}/explores/*",
            f"//looker-hub/{name}/dashboards/*",
            "views/*",
            "explores/*",
            "dashboards/*",
        ],
    }

    path = spoke_path / name / f"{name}.model.lkml"
    path.write_text(lkml.dump(model_defn))

    return path


def configure_model(sdk: looker_sdk.methods.Looker31SDK, model_name: str):
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
        looker_sdk.models.WriteLookmlModel(
            allowed_db_connection_names=["telemetry"],
            name=model_name,
            project_name="spoke-default",
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
            _id, looker_sdk.models.WriteModelSet(models=list(models) + [model_name])
        )


def generate_directories(
    namespaces: Dict[str, NamespaceDict], spoke_dir: Path, sdk_setup=False
):
    """Generate directories and model for a namespace, if it doesn't exist."""
    sdk = looker_sdk.init31()
    logging.info("Looker SDK 3.1 initialized successfully.")

    existing_dirs = {p.name for p in spoke_dir.iterdir()}
    for namespace, defn in namespaces.items():
        if namespace in existing_dirs:
            continue

        (spoke_dir / namespace).mkdir()
        for dirname in ("views", "explores", "dashboards"):
            (spoke_dir / namespace / dirname).mkdir()
            (spoke_dir / namespace / dirname / ".gitkeep").touch()

        generate_model(spoke_dir, namespace, defn)

        if sdk_setup:
            configure_model(sdk, namespace)


@click.command(help=__doc__)
@click.option(
    "--namespaces",
    default="namespaces.yaml",
    type=click.File(),
    help="Path to the namespaces.yaml file.",
)
@click.option(
    "--spoke-dir",
    default="looker-spoke-default",
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    help="Directory containing the Looker spoke.",
)
def update_spoke(namespaces, spoke_dir):
    """Generate updates to spoke project."""
    _namespaces = yaml.safe_load(namespaces)
    sdk_setup = setup_env_with_looker_creds()
    generate_directories(_namespaces, Path(spoke_dir), sdk_setup)
