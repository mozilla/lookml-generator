"""Generic explore type."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import lkml
from google.cloud import bigquery

from ..views.lookml_utils import escape_filter_expr, slug_to_title


@dataclass
class Explore:
    """A generic explore."""

    name: str
    views: Dict[str, str]
    views_path: Optional[Path] = None
    defn: Optional[Dict[str, str]] = None
    type: str = field(init=False)

    def to_dict(self) -> dict:
        """Explore instance represented as a dict."""
        return {self.name: {"type": self.type, "views": self.views}}

    def to_lookml(
        self, client: bigquery.Client, v1_name: Optional[str], data: Dict = {}
    ) -> List[Dict[str, Any]]:
        """
        Generate LookML for this explore.

        Any generation done in dependent explore's
        `_to_lookml` takes precedence over these fields.
        """
        base_lookml = {}
        base_view_name = next(
            (
                view_name
                for view_type, view_name in self.views.items()
                if view_type == "base_view"
            )
        )
        for view_type, view in self.views.items():
            # We look at our dependent views to see if they have a
            # "submission" field. Dependent views are any that are:
            # - base_view
            # - extended_view*
            #
            # We do not want to look at joined views. Those should be
            # labeled as:
            # - join*
            #
            # If they have a submission field, we filter on the date.
            # This allows for filter queries to succeed.
            if "join" in view_type:
                continue
            if self._get_view_has_submission(view):
                base_lookml[
                    "sql_always_where"
                ] = f"${{{base_view_name}.submission_date}} >= '2010-01-01'"

        # We only update the first returned explore
        new_lookml = self._to_lookml(client, v1_name, data)
        base_lookml.update(new_lookml[0])
        new_lookml[0] = base_lookml

        return new_lookml

    def _to_lookml(
        self,
        client: bigquery.Client,
        v1_name: Optional[str],
        data: Dict = {},
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("Only implemented in subclasses")

    def get_dependent_views(self) -> List[str]:
        """Get views this explore is dependent on."""
        return [
            view
            for _type, view in self.views.items()
            if not _type.startswith("extended")
        ]

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> Explore:
        """Get an instance of an explore from a namespace definition."""
        raise NotImplementedError("Only implemented in subclasses")

    def get_view_lookml(self, view: str) -> dict:
        """Get the LookML for a view."""
        if self.views_path is not None:
            return lkml.load((self.views_path / f"{view}.view.lkml").read_text())
        raise Exception("Missing view path for get_view_lookml")

    def get_unnested_fields_joins_lookml(
        self,
    ) -> list:
        """Get the LookML for joining unnested fields."""
        views_lookml = self.get_view_lookml(self.views["base_view"])
        views: List[str] = [view["name"] for view in views_lookml["views"]]
        parent_base_name = views_lookml["views"][0]["name"]

        extended_views: List[str] = []
        if "extended_view" in self.views:
            # check for extended views
            extended_views_lookml = self.get_view_lookml(self.views["extended_view"])
            extended_views = [view["name"] for view in extended_views_lookml["views"]]

            views_lookml.update(extended_views_lookml)
            views += extended_views

        joins = []
        for view in views_lookml["views"][1:]:
            view_name = view["name"]
            # get repeated, nested fields that exist as separate views in lookml
            base_name, metric = self._get_base_name_and_metric(
                view_name=view_name, views=views
            )
            metric_name = view_name
            metric_label = slug_to_title(metric_name)

            if view_name in extended_views:
                # names of extended views are overriden by the name of the view that is extending them
                metric_label = slug_to_title(
                    metric_name.replace(base_name, parent_base_name)
                )
                base_name = parent_base_name

            joins.append(
                {
                    "name": view_name,
                    "view_label": metric_label,
                    "relationship": "one_to_many",
                    "sql": (
                        f"LEFT JOIN UNNEST(${{{base_name}.{metric}}}) AS {metric_name} "
                    ),
                }
            )

        return joins

    def _get_default_channel(self, view: str) -> Optional[str]:
        channel_params = [
            param
            for _view_defn in self.get_view_lookml(view)["views"]
            for param in _view_defn.get("parameters", [])
            if _view_defn["name"] == view and param["name"] == "channel"
        ]

        if channel_params:
            allowed_values = channel_params[0]["allowed_values"]
            default_value = next(
                (value for value in allowed_values if value["label"] == "Release"),
                allowed_values[0],
            )["value"]

            return escape_filter_expr(default_value)
        return None

    def _get_base_name_and_metric(
        self, view_name: str, views: List[str]
    ) -> Tuple[str, str]:
        """
        Get base view and metric names.

        Returns the the name of the base view and the metric based on the
        passed `view_name` and existing views.

        The names are resolved in a backwards fashion to account for
        repeated nested fields that might contain other nested fields.
        For example:

        view: sync {
            [...]
            dimension: payload__events {
                sql: ${TABLE}.payload.events ;;
            }
        }

        view: sync__payload__events {
            [...]
            dimension: f5_ {
                sql: ${TABLE}.f5_ ;;
            }
        }

        view: sync__payload__events__f5_ {
            [...]
        }

        For these nested views to get translated to the following joins, the names
        need to be resolved backwards:

        join: sync__payload__events {
            relationship: one_to_many
            sql: LEFT JOIN UNNEST(${sync.payload__events}) AS sync__payload__events ;;
        }

        join: sync__payload__events__f5_ {
            relationship: one_to_many
            sql: LEFT JOIN UNNEST(${sync__payload__events.f5_}) AS sync__payload__events__f5_ ;;
        }
        """
        split = view_name.split("__")
        for index in range(len(split) - 1, 0, -1):
            base_view = "__".join(split[:index])
            metric = "__".join(split[index:])
            if base_view in views:
                return (base_view, metric)
        raise Exception(f"Cannot get base name and metric from view {view_name}")

    def _get_view_has_submission(self, view: str) -> bool:
        return (
            len(
                [
                    dim
                    for _view_defn in self.get_view_lookml(view)["views"]
                    for dim in _view_defn.get("dimension_groups", [])
                    if _view_defn["name"] == view and dim["name"] == "submission"
                ]
            )
            > 0
        )

    def get_required_filters(self, view_name: str) -> List[Dict[str, str]]:
        """Get required filters for this view."""
        filters = []
        view = self.views[view_name]

        # Add a default filter on channel, if it's present in the view
        default_channel = self._get_default_channel(view)
        if default_channel is not None:
            filters.append({"channel": default_channel})

        # Add submission filter, if present in the view
        if self._get_view_has_submission(view):
            filters.append({"submission_date": "28 days"})

        return filters

    def __eq__(self, other) -> bool:
        """Check for equality with other View."""

        def comparable_dict(d):
            return tuple(sorted(d.items()))

        if isinstance(other, Explore):
            return (
                self.name == other.name
                and comparable_dict(self.views) == comparable_dict(other.views)
                and self.type == other.type
            )
        return False
