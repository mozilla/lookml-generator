"""Glean Ping explore type."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery
from mozilla_schema_generator.glean_ping import GleanPing

from ..views import GleanPingView, View
from .ping_explore import PingExplore


class GleanPingExplore(PingExplore):
    """A Glean Ping Table explore."""

    type: str = "glean_ping_explore"

    def _to_lookml(
        self, client: bigquery.Client, v1_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Generate LookML to represent this explore."""
        repo = next((r for r in GleanPing.get_repos() if r["name"] == v1_name))
        glean_app = GleanPing(repo)
        # convert ping description indexes to snake case, as we already have
        # for the explore name
        ping_descriptions = {
            k.replace("-", "_"): v for k, v in glean_app.get_ping_descriptions().items()
        }
        # collapse whitespace in the description so the lookml looks a little better
        ping_description = " ".join(ping_descriptions.get(self.name, "").split())
        views_lookml = self.get_view_lookml(self.views["base_view"])

        # The first view, by convention, is always the base view with the
        # majority of the dimensions from the top level.
        base = views_lookml["views"][0]
        base_name = base["name"]

        joins = []
        for view in views_lookml["views"][1:]:
            if view["name"].startswith("suggest__"):
                continue
            view_name = view["name"]
            metric = "__".join(view["name"].split("__")[1:])

            if "labeled_counter" in metric:
                joins.append(
                    {
                        "name": view_name,
                        "relationship": "one_to_many",
                        "sql": (
                            f"LEFT JOIN UNNEST(${{{base_name}.{metric}}}) AS {view_name} "
                            f"ON ${{{base_name}.document_id}} = ${{{view_name}.document_id}}"
                        ),
                    }
                )
            else:
                if metric.startswith("metrics__"):
                    continue

                try:
                    # get repeated, nested fields that exist as separate views in lookml
                    base_name, metric = self._get_base_name_and_metric(
                        view_name=view_name,
                        views=[v["name"] for v in views_lookml["views"]],
                    )
                    metric_name = view_name

                    joins.append(
                        {
                            "name": view_name,
                            "relationship": "one_to_many",
                            "sql": (
                                f"LEFT JOIN UNNEST(${{{base_name}.{metric}}}) AS {metric_name} "
                            ),
                        }
                    )
                except Exception:
                    # ignore nested views that cannot be joined on to the base view
                    continue

        base_explore = {
            "name": self.name,
            # list the base explore first by prefixing with a space
            "view_label": f" {self.name.title()}",
            "description": f"Explore for the {self.name} ping. {ping_description}",
            "view_name": self.views["base_view"],
            "always_filter": {
                "filters": self.get_required_filters("base_view"),
            },
            "joins": joins,
        }

        suggests = []
        for view in views_lookml["views"][1:]:
            if not view["name"].startswith("suggest__"):
                continue
            suggests.append({"name": view["name"], "hidden": "yes"})

        return [base_explore] + suggests

    @staticmethod
    def from_views(views: List[View]) -> Iterator[PingExplore]:
        """Generate all possible GleanPingExplores from the views."""
        for view in views:
            if view.view_type == GleanPingView.type:
                yield GleanPingExplore(view.name, {"base_view": view.name})

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> GleanPingExplore:
        """Get an instance of this explore from a name and dictionary definition."""
        return GleanPingExplore(name, defn["views"], views_path)
