"""Classes to describe Looker views."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Union

OMIT_VIEWS = {"deletion_request"}


class View(object):
    """A generic Looker View."""

    @classmethod
    def from_db_views(klass, variants: dict, db_views: dict) -> dict:
        """Get Looker views from db views and app variants."""
        raise NotImplementedError()


class PingView(View):
    """A view on a ping table."""

    @classmethod
    def from_db_views(klass, variants: dict, db_views: dict):
        """Get Looker views from db views and app variants."""
        views = defaultdict(list)
        for app in variants:
            if app.get("deprecated"):
                continue
            is_release = app.get("app_channel") == "release"
            dataset_id = app["bq_dataset_family"]
            for view_id, references in db_views[dataset_id].items():
                if view_id in OMIT_VIEWS:
                    continue
                table: Dict[str, Union[str, bool]] = {
                    "table": f"mozdata.{dataset_id}.{view_id}"
                }
                if "app_channel" in app:
                    table["channel"] = app["app_channel"]
                if len(references) == 1 and references[0][-2] == f"{dataset_id}_stable":
                    # view references a single table in the stable dataset
                    table["is_ping_table"] = True
                elif not is_release:
                    continue  # ignore non-ping tables from non-release datasets
                views[view_id].append(table)

        return views


class GrowthAccountingView(View):
    """A view for growth accounting measures."""

    @classmethod
    def from_db_views(klass, variants: dict, db_views: dict):
        """Get Growth Accounting Views from db views and app variants."""
        return {}


view_types = {
    "ping_view": PingView,
    "growth_accounting": GrowthAccountingView,
}
