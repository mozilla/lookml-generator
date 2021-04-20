"""CLI for generating content folders for Looker namespaces."""
import logging
import os

import click
import looker_sdk
import yaml

INSTANCE = "mozilladev"


@click.command(help=__doc__)
@click.option(
    "--namespaces",
    default="namespaces.yaml",
    type=click.File(),
    help="Path to a yaml namespaces file",
)
def generate_content(namespaces):
    """Generate content folders."""
    client_id = os.environ.get("LOOKER_API_CLIENT_ID")
    client_secret = os.environ.get("LOOKER_API_CLIENT_SECRET")

    os.environ["LOOKERSDK_BASE_URL"] = f"https://{INSTANCE}.cloud.looker.com"
    os.environ["LOOKERSDK_API_VERSION"] = "3.1"
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true"
    os.environ["LOOKERSDK_TIMEOUT"] = "120"
    os.environ["LOOKERSDK_CLIENT_ID"] = client_id
    os.environ["LOOKERSDK_CLIENT_SECRET"] = client_secret

    sdk = looker_sdk.init31()
    logging.info("Looker SDK 3.1 initialized successfully.")

    shared_folders = list(sdk.search_folders(name="home"))
    if len(shared_folders) != 1:
        raise click.ClickException(
            f"Found wrong number of shared folders: {shared_folders}"
        )

    shared_folder_id = shared_folders[0].id

    for namespace, defn in yaml.safe_load(namespaces).items():
        pretty_name = defn["canonical_app_name"]

        try:
            folders = sdk.search_folders(name=pretty_name, parent_id=shared_folder_id)
            if len(folders) != 1:
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

        content_metadatas = sdk.all_content_metadatas(parent_id=shared_folder_id)
        for content_metadata in content_metadatas:
            if content_metadata.folder_id != folder.id:
                continue

            # Set the folder to not inherit from parent
            sdk.update_content_metadata(
                content_metadata.id, looker_sdk.models.WriteContentMeta(inherits=False)
            )

            content_metadata_accesses = sdk.all_content_metadata_accesses(
                content_metadata.id
            )

            # Delete all existing content_metadata_accesses
            for content_metadata_access in content_metadata_accesses:
                sdk.delete_content_metadata_access(content_metadata_access.id)

            # Add read access for all users (group id=1)
            sdk.create_content_metadata_access(
                looker_sdk.models.ContentMetaGroupUser(
                    content_metadata_id=content_metadata.id,
                    permission_type="view",
                    group_id=1,
                )
            )

            # Add write access for admins
            admin_roles = sdk.search_roles(name="Admin")
            if len(admin_roles) != 1:
                raise click.ClickException(
                    f"Found wrong number of admin roles: {admin_roles}"
                )

            admins = sdk.role_users(admin_roles[0].id)
            write_access_users = {admin.id for admin in admins}

            # Write access for all owners
            for owner in defn["owners"]:
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
                        content_metadata_id=content_metadata.id,
                        permission_type="edit",
                        user_id=user_id,
                    )
                )
