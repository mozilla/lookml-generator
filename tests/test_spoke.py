from unittest.mock import patch

import lkml
import pytest

from generator.spoke import generate_directories


@pytest.fixture()
def namespaces() -> dict:
    return {
        "glean-app": {
            "canonical_app_name": "Glean App",
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


@patch("generator.spoke.looker_sdk")
def test_generate_directories(looker_sdk, namespaces, tmp_path):
    generate_directories(namespaces, tmp_path)
    dirs = list(tmp_path.iterdir())
    assert dirs == [tmp_path / "glean-app"]

    app_path = tmp_path / "glean-app/"
    sub_dirs = set(app_path.iterdir())
    assert sub_dirs == {
        app_path / "views",
        app_path / "explores",
        app_path / "dashboards",
        app_path / "glean-app.model.lkml",
    }

    sdk = looker_sdk.init31()
    sdk.create_lookml_model.assert_called_once()


@patch("generator.spoke.looker_sdk")
def test_existing_dir(looker_sdk, namespaces, tmp_path):
    generate_directories(namespaces, tmp_path)
    tmp_file = tmp_path / "glean-app" / "tmp-file"
    tmp_file.write_text("hello, world")

    generate_directories(namespaces, tmp_path)

    # We shouldn't overwrite this dir
    assert tmp_file.is_file()


@patch("generator.spoke.looker_sdk")
def test_generate_model(looker_sdk, namespaces, tmp_path):
    generate_directories(namespaces, tmp_path)
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
    actual = lkml.load((tmp_path / "glean-app" / "glean-app.model.lkml").read_text())
    assert expected == actual
