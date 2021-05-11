from unittest.mock import Mock, patch

import click
import pytest

from generator.content import generate_folders


@pytest.fixture
def namespaces():
    return {
        "burnham": {
            "pretty_name": "Burnham",
            "glean_app": True,
            "owners": ["owner@mozilla.com"],
            "explores": [],
            "views": [],
        }
    }


@pytest.fixture
def namespace_allowlist():
    return {
        "burnham": {
            "owners": [
                "owner2@mozilla.com",
            ],
        }
    }


@patch("generator.content.looker_sdk")
def test_more_than_one_namespace_folder(looker_sdk, namespaces):
    sdk = looker_sdk.init31()
    sdk.search_folders.return_value = [Mock(id=2), Mock(id=3)]
    with pytest.raises(click.ClickException):
        generate_folders(namespaces)


@patch("generator.content.looker_sdk")
def test_new_namespace(looker_sdk, namespaces, namespace_allowlist):
    sdk = looker_sdk.init31()

    def search_folders(name=None, parent_id=None):
        if name == "home":
            return [Mock(id=1)]
        else:
            return []

    sdk.search_folders.side_effect = search_folders
    sdk.create_folder.return_value = Mock(id=3)
    sdk.all_content_metadatas.return_value = [
        Mock(id=4, folder_id=3),
        Mock(id=100, folder_id=1),
    ]
    sdk.all_content_metadata_accesses.return_value = [Mock(id=5)]
    sdk.search_roles.return_value = [Mock(id=6)]
    sdk.role_users.return_value = [Mock(id=7)]
    sdk.search_users.side_effect = [[Mock(id=8)], [Mock(id=0)]]

    # Do the thing, Julie
    generate_folders(namespaces, namespace_allowlist)

    sdk.search_folders.assert_any_call(name="home")
    sdk.search_folders.assert_any_call(name="Burnham", parent_id="1")
    sdk.create_folder.assert_called_once()
    sdk.all_content_metadatas.assert_called_once_with(parent_id=1)
    sdk.update_content_metadata.assert_called_once()
    sdk.all_content_metadata_accesses.assert_called_once_with(4)
    sdk.delete_content_metadata_access.assert_called_once_with(5)
    sdk.search_roles.assert_called_once_with(name="Admin")
    sdk.role_users.assert_called_once_with(6)
    sdk.search_users.assert_any_call(email="owner@mozilla.com")
    sdk.search_users.assert_any_call(email="owner2@mozilla.com")

    # Called three times: for all users, for admin, and for owner
    assert len(sdk.create_content_metadata_access.call_args_list) == 4


@patch("generator.content.looker_sdk")
def test_existing_namespace(looker_sdk, namespaces):
    sdk = looker_sdk.init31()

    def search_folders(name=None, parent_id=None):
        if name == "home":
            return [Mock(id=1)]
        else:
            return [Mock(id=2)]

    sdk.search_folders.side_effect = search_folders
    sdk.all_content_metadatas.return_value = [
        Mock(id=4, folder_id=2),
        Mock(id=100, folder_id=1),
    ]
    sdk.all_content_metadata_accesses.return_value = [Mock(id=5)]
    sdk.search_roles.return_value = [Mock(id=6)]
    sdk.role_users.return_value = [Mock(id=7)]
    sdk.search_users.return_value = [Mock(id=8)]

    # Do the thing, Julie
    generate_folders(namespaces)

    sdk.search_folders.assert_any_call(name="home")
    sdk.search_folders.assert_any_call(name="Burnham", parent_id="1")
    sdk.create_folder.assert_not_called()
    sdk.all_content_metadatas.assert_called_once_with(parent_id=1)
    sdk.update_content_metadata.assert_called_once()
    sdk.all_content_metadata_accesses.assert_called_once_with(4)
    sdk.delete_content_metadata_access.assert_called_once_with(5)
    sdk.search_roles.assert_called_once_with(name="Admin")
    sdk.role_users.assert_called_once_with(6)
    sdk.search_users.assert_called_once_with(email="owner@mozilla.com")

    # Called three times: for all users, for admin, and for owner
    assert len(sdk.create_content_metadata_access.call_args_list) == 3


@patch("generator.content.looker_sdk")
def test_wrong_number_of_namespace_folders(looker_sdk, namespaces):
    sdk = looker_sdk.init31()

    def search_folders(name=None, parent_id=None):
        if name == "home":
            return [Mock(id=1)]
        else:
            return [Mock(id=2), Mock(3)]

    sdk.search_folders.side_effect = search_folders

    with pytest.raises(click.ClickException) as exc:
        generate_folders(namespaces)
        assert "namespace folders" in str(exc.value)


@patch("generator.content.looker_sdk")
def test_missing_content_metadata(looker_sdk, namespaces):
    sdk = looker_sdk.init31()

    def search_folders(name=None, parent_id=None):
        if name == "home":
            return [Mock(id=1)]
        else:
            return [Mock(id=2)]

    sdk.search_folders.side_effect = search_folders
    sdk.all_content_metadatas.return_value = [
        Mock(id=4, folder_id=2),
        Mock(id=5, folder_id=2),
    ]

    with pytest.raises(click.ClickException) as exc:
        generate_folders(namespaces)
        assert "content metadata" in str(exc.value)


@patch("generator.content.looker_sdk")
def test_wrong_number_of_admin_roles(looker_sdk, namespaces):
    sdk = looker_sdk.init31()

    def search_folders(name=None, parent_id=None):
        if name == "home":
            return [Mock(id=1)]
        else:
            return [Mock(id=2)]

    sdk.search_folders.side_effect = search_folders
    sdk.all_content_metadatas.return_value = [
        Mock(id=4, folder_id=2),
        Mock(id=100, folder_id=1),
    ]
    sdk.all_content_metadata_accesses.return_value = [Mock(id=5)]
    sdk.search_roles.return_value = [Mock(id=6), Mock(id=100)]

    # Do the thing, Julie
    with pytest.raises(click.ClickException) as exc:
        generate_folders(namespaces)
        assert "admin roles" in str(exc.value)


@patch("generator.content.looker_sdk")
def test_more_than_one_email_match(looker_sdk, namespaces):
    sdk = looker_sdk.init31()

    def search_folders(name=None, parent_id=None):
        if name == "home":
            return [Mock(id=1)]
        else:
            return [Mock(id=2)]

    sdk.search_folders.side_effect = search_folders
    sdk.all_content_metadatas.return_value = [
        Mock(id=4, folder_id=2),
        Mock(id=100, folder_id=1),
    ]
    sdk.all_content_metadata_accesses.return_value = [Mock(id=5)]
    sdk.search_roles.return_value = [Mock(id=6)]
    sdk.role_users.return_value = [Mock(id=7)]
    sdk.search_users.return_value = [Mock(id=8), Mock(id=9)]

    with pytest.raises(click.ClickException) as exc:
        generate_folders(namespaces)
        assert "more than one user with email" in str(exc.value)
