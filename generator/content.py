"""CLI for generating content folders for Looker namespaces."""
import logging
import os

import click
import looker_sdk
import yaml


def _setup_env_with_looker_creds():
    client_id = os.environ["LOOKER_API_CLIENT_ID"]
    client_secret = os.environ["LOOKER_API_CLIENT_SECRET"]
    instance = os.environ["LOOKER_INSTANCE_URI"]

    os.environ["LOOKERSDK_BASE_URL"] = instance
    os.environ["LOOKERSDK_API_VERSION"] = "3.1"
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true"
    os.environ["LOOKERSDK_TIMEOUT"] = "120"
    os.environ["LOOKERSDK_CLIENT_ID"] = client_id
    os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret


def generate_folders(namespaces: dict):
    """Generate folders and ACLs for namespaces."""
    sdk = looker_sdk.init31()
    logging.info("Looker SDK 3.1 initialized successfully.")

    shared_folders = sdk.search_folders(name="home")
    if len(shared_folders) != 1:
        raise click.ClickException(
            f"Found wrong number of shared folders: {shared_folders}"
        )

    shared_folder_id = shared_folders[0].id
    if shared_folder_id is None:
        raise click.ClickException("Error: Shared folder id is missing")

    for namespace, defn in namespaces.items():
        pretty_name = defn["canonical_app_name"]
        owners = defn["owners"]

        try:
            folders = sdk.search_folders(name=pretty_name, parent_id=shared_folder_id)
            if len(folders) > 1:
                raise click.ClickException(
                    f"Found wrong number of namespace folders: {folders}"
                )
            folder = folders[0]
        except IndexError:
            logging.info(f"Creating folder {pretty_name}")
            folder = sdk.create_folder(
                looker_sdk.models.CreateFolder(
                    name=pretty_name, parent_id=shared_folder_id
                )
            )

        content_metadatas = sdk.all_content_metadatas(parent_id=int(shared_folder_id))
        content_metadata_folder = [
            cm for cm in content_metadatas if cm.folder_id == folder.id
        ]

        if len(content_metadata_folder) != 1:
            raise click.ClickException(
                f"Found wrong number of content metadata: {content_metadata_folder}"
            )

        content_metadata = content_metadata_folder[0]
        if content_metadata.id is None:
            raise click.ClickException(
                f"Error: Content metadata id is missing in {content_metadata}"
            )

        # Set the folder to not inherit from parent
        sdk.update_content_metadata(
            int(content_metadata.id), looker_sdk.models.WriteContentMeta(inherits=False)
        )

        content_metadata_accesses = sdk.all_content_metadata_accesses(
            content_metadata.id
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
            sdk.delete_content_metadata_access(int(content_metadata_access.id))

        # Add read access for all users (group id=1)
        sdk.create_content_metadata_access(
            looker_sdk.models.ContentMetaGroupUser(
                content_metadata_id=str(content_metadata.id),
                permission_type=looker_sdk.models.PermissionType.view,
                group_id=1,
            )
        )

        # Add write access for admins
        admin_roles = sdk.search_roles(name="Admin")
        if len(admin_roles) != 1:
            raise click.ClickException(
                f"Found wrong number of admin roles: {admin_roles}"
            )

        admin_role = admin_roles[0]
        if admin_role.id is None:
            raise click.ClickException(f"Admin role id is missing in {admin_role}")

        admins = sdk.role_users(admin_role.id)
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
            sdk.create_content_metadata_access(
                looker_sdk.models.ContentMetaGroupUser(
                    content_metadata_id=str(content_metadata.id),
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
    _setup_env_with_looker_creds()
    generate_folders(yaml.safe_load(namespaces))
