"""CLI for generating content folders for Looker namespaces."""
import logging
import os
from typing import Any, Sequence

import click
import looker_sdk


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
    os.environ["LOOKERSDK_API_VERSION"] = "3.1"
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true"
    os.environ["LOOKERSDK_TIMEOUT"] = "120"
    os.environ["LOOKERSDK_CLIENT_ID"] = client_id
    os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret

    return True


def _get_id_from_list(arr: Sequence[Any], item: str) -> int:
    if len(arr) != 1:
        raise click.ClickException(f"Found wrong number of {item} in {arr}")
    first = arr[0]
    if first.id is None:
        raise click.ClickException(f"{item} missing id in {first}")
    return int(first.id)


def generate_folders(namespaces: dict):
    """Generate folders and ACLs for namespaces."""
    sdk = looker_sdk.init31()
    logging.info("Looker SDK 3.1 initialized successfully.")

    shared_folders = sdk.search_folders(name="home")
    shared_folder_id = _get_id_from_list(shared_folders, "Shared folders")

    for namespace, defn in namespaces.items():
        pretty_name = defn["pretty_name"]
        owners = defn["owners"]

        try:
            folders = sdk.search_folders(
                name=pretty_name, parent_id=str(shared_folder_id)
            )
            folder_id = _get_id_from_list(folders, "Folders")
        except click.ClickException as e:
            if str(e).startswith("Item missing id"):
                raise e

            logging.info(f"Creating folder Shared/{pretty_name}")
            folder_id_untyped = sdk.create_folder(
                looker_sdk.models.CreateFolder(
                    name=pretty_name, parent_id=str(shared_folder_id)
                )
            ).id
            if folder_id_untyped is None:
                raise click.ClickException("Error: Folder missing id")
            folder_id = int(folder_id_untyped)

        content_metadatas = sdk.all_content_metadatas(parent_id=shared_folder_id)
        content_metadata_folder = [
            cm
            for cm in content_metadatas
            if cm.folder_id is not None and int(cm.folder_id) == folder_id
        ]

        content_metadata_id = _get_id_from_list(
            content_metadata_folder, "Content metadata folders"
        )

        # Set the folder to not inherit from parent
        logging.info(
            f"Updating content metadata to not inherit for {content_metadata_folder[0]}"
        )
        sdk.update_content_metadata(
            int(content_metadata_id), looker_sdk.models.WriteContentMeta(inherits=False)
        )

        content_metadata_accesses = sdk.all_content_metadata_accesses(
            int(content_metadata_id)
        )

        # Delete all existing access controls
        for content_metadata_access in content_metadata_accesses:
            if content_metadata_access.id is None:
                raise click.ClickException(
                    (
                        f"Error: Content metadata accesses id is missing"
                        f"in {content_metadata_accesses}"
                    )
                )
            logging.info(f"Deleting content metadata access {content_metadata_access}")
            sdk.delete_content_metadata_access(int(content_metadata_access.id))

        # Add read access for all users (group id=1)
        logging.info(f"Adding read access for all users for {content_metadata_id}")
        sdk.create_content_metadata_access(
            looker_sdk.models.ContentMetaGroupUser(
                content_metadata_id=str(content_metadata_id),
                permission_type=looker_sdk.models.PermissionType.view,
                group_id=1,
            )
        )

        # Add write access for admins
        admin_roles = sdk.search_roles(name="Admin")
        admin_role_id = _get_id_from_list(admin_roles, "Admin roles")

        admins = sdk.role_users(admin_role_id)
        write_access_users = {admin.id for admin in admins}

        # Write access for all owners
        for owner in owners:
            email_users = sdk.search_users(email=owner)
            if len(email_users) > 1:
                raise click.ClickException(
                    f"Found more than one user with email {owner}"
                )
            elif len(email_users) == 1:
                write_access_users.add(email_users[0].id)

        for user_id in write_access_users:
            logging.info(f"Adding write access for user id {user_id}")
            sdk.create_content_metadata_access(
                looker_sdk.models.ContentMetaGroupUser(
                    content_metadata_id=str(content_metadata_id),
                    permission_type=looker_sdk.models.PermissionType.edit,
                    user_id=user_id,
                )
            )


@click.command(help=__doc__)
@click.option(
    "--namespaces",
    default="namespaces.yaml",
    type=click.File(),
    help="Path to a yaml namespaces file",
)
def generate_content(namespaces):
    """Generate content folders."""
    setup_env_with_looker_creds()
    # generate_folders(yaml.safe_load(namespaces))
