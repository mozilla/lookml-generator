import os
from unittest.mock import Mock, patch

import lkml
import looker_sdk as _looker_sdk
import pytest

from generator.spoke import generate_directories

from .utils import print_and_test


@pytest.fixture()
def namespaces() -> dict:
    return {
        "glean-app": {
            "pretty_name": "Glean App",
            "glean_app": True,
            "spoke": "looker-spoke-default",
            "views": {
                "baseline": {
                    "type": "ping_view",
                    "tables": [
                        {
                            "channel": "release",
                            "table": "mozdata.glean_app.baseline",
                        }
                    ],
                }
            },
            "explores": {
                "baseline": {"type": "ping_explore", "views": {"base_view": "baseline"}}
            },
        }
    }


@pytest.fixture
def custom_namespaces():
    return {
        "custom": {
            "glean_app": False,
            "spoke": "looker-spoke-private",
            "connection": "bigquery-oauth",
            "owners": ["custom-owner@allizom.com", "custom-owner2@allizom.com"],
            "pretty_name": "Custom",
            "views": {
                "baseline": {
                    "tables": [
                        {"channel": "release", "table": "mozdata.custom.baseline"}
                    ],
                    "type": "ping_view",
                }
            },
        }
    }


@patch("generator.spoke.looker_sdk")
@patch.dict(os.environ, {"LOOKER_INSTANCE_URI": "https://mozilladev.cloud.looker.com"})
def test_generate_directories(looker_sdk, namespaces, tmp_path):
    sdk = looker_sdk.init31()
    sdk.search_model_sets.return_value = [Mock(models=["model"], id=1)]
    sdk.lookml_model.side_effect = _looker_sdk.error.SDKError("msg")
    looker_sdk.error = Mock(SDKError=_looker_sdk.error.SDKError)

    generate_directories(namespaces, tmp_path, True)
    dirs = list((tmp_path / "looker-spoke-default").iterdir())
    assert dirs == [tmp_path / "looker-spoke-default" / "glean-app"]

    app_path = tmp_path / "looker-spoke-default" / "glean-app/"
    sub_dirs = set(app_path.iterdir())
    assert sub_dirs == {
        app_path / "views",
        app_path / "explores",
        app_path / "dashboards",
        app_path / "glean-app.model.lkml",
    }

    sdk.create_lookml_model.assert_called_once()
    sdk.update_model_set.assert_called_once()


@patch("generator.spoke.looker_sdk")
def test_generate_directories_no_sdk(looker_sdk, namespaces, tmp_path):
    sdk = looker_sdk.init31()
    sdk.search_model_sets.return_value = [Mock(models=["model"], id=1)]

    generate_directories(namespaces, tmp_path, False)
    dirs = list((tmp_path / "looker-spoke-default").iterdir())
    assert dirs == [tmp_path / "looker-spoke-default" / "glean-app"]

    app_path = tmp_path / "looker-spoke-default" / "glean-app"
    sub_dirs = set(app_path.iterdir())
    assert sub_dirs == {
        app_path / "views",
        app_path / "explores",
        app_path / "dashboards",
        app_path / "glean-app.model.lkml",
    }

    assert (app_path / "dashboards" / ".gitkeep").exists()

    sdk.create_lookml_model.assert_not_called()


@patch("generator.spoke.looker_sdk")
@patch.dict(os.environ, {"LOOKER_INSTANCE_URI": "https://mozilladev.cloud.looker.com"})
def test_existing_dir(looker_sdk, namespaces, tmp_path):
    sdk = looker_sdk.init31()
    sdk.search_model_sets.return_value = [Mock(models=["model"], id=1)]

    generate_directories(namespaces, tmp_path, True)
    tmp_file = tmp_path / "looker-spoke-default" / "glean-app" / "tmp-file"
    tmp_file.write_text("hello, world")

    generate_directories(namespaces, tmp_path)

    # We shouldn't overwrite this dir
    assert tmp_file.is_file()


@patch("generator.spoke.looker_sdk")
@patch.dict(os.environ, {"LOOKER_INSTANCE_URI": "https://mozilla.cloud.looker.com"})
def test_generate_model(looker_sdk, namespaces, tmp_path):
    sdk = looker_sdk.init31()
    sdk.search_model_sets.side_effect = [[Mock(models=["model"], id=1)]]
    sdk.lookml_model.side_effect = _looker_sdk.error.SDKError("msg")
    looker_sdk.error = Mock(SDKError=_looker_sdk.error.SDKError)

    write_model = Mock()
    looker_sdk.models.WriteModelSet.return_value = write_model

    generate_directories(namespaces, tmp_path, True)
    expected = {
        "connection": "telemetry",
        "label": "Glean App",
        "includes": [
            "//looker-hub/glean-app/explores/*",
            "//looker-hub/glean-app/dashboards/*",
            "views/*",
            "explores/*",
            "dashboards/*",
        ],
    }
    actual = lkml.load(
        (
            tmp_path / "looker-spoke-default" / "glean-app" / "glean-app.model.lkml"
        ).read_text()
    )
    assert expected == actual

    looker_sdk.models.WriteModelSet.assert_any_call(models=["model", "glean-app"])
    assert looker_sdk.models.WriteModelSet.call_count == 1

    sdk.update_model_set.assert_any_call(1, write_model)


@patch("generator.spoke.looker_sdk")
@patch.dict(os.environ, {"LOOKER_INSTANCE_URI": "https://mozilladev.cloud.looker.com"})
def test_alternate_connection(looker_sdk, custom_namespaces, tmp_path):
    sdk = looker_sdk.init31()
    sdk.search_model_sets.return_value = [Mock(models=["model"], id=1)]
    sdk.lookml_model.side_effect = _looker_sdk.error.SDKError("msg")
    looker_sdk.error = Mock(SDKError=_looker_sdk.error.SDKError)

    write_model = Mock()
    looker_sdk.models.WriteLookmlModel.return_value = write_model

    generate_directories(custom_namespaces, tmp_path, True)
    dirs = list((tmp_path / "looker-spoke-private").iterdir())
    assert dirs == [tmp_path / "looker-spoke-private" / "custom"]

    app_path = tmp_path / "looker-spoke-private" / "custom"
    sub_dirs = set(app_path.iterdir())
    assert sub_dirs == {
        app_path / "views",
        app_path / "explores",
        app_path / "dashboards",
        app_path / "custom.model.lkml",
    }

    expected = {
        "connection": "bigquery-oauth",
        "label": "Custom",
        "includes": [
            "//looker-hub/custom/explores/*",
            "//looker-hub/custom/dashboards/*",
            "views/*",
            "explores/*",
            "dashboards/*",
        ],
    }
    actual = lkml.load(
        (tmp_path / "looker-spoke-private" / "custom" / "custom.model.lkml").read_text()
    )
    print_and_test(expected, actual)

    looker_sdk.models.WriteLookmlModel.assert_called_with(
        allowed_db_connection_names=["bigquery-oauth"],
        name="custom",
        project_name="spoke-private",
    )
    sdk.create_lookml_model.assert_called_with(write_model)
    sdk.update_model_set.assert_called_once()
