"""Class to describe Operational Monitoring Dashboard."""
from __future__ import annotations

from typing import Any, Dict, List

from ..views import lookml_utils
from .dashboard import Dashboard


class OperationalMonitoringDashboard(Dashboard):
    """An Operational Monitoring dashboard."""

    type: str = "operational_monitoring_dashboard"

    def __init__(
        self,
        title: str,
        name: str,
        layout: str,
        namespace: str,
        defn: List[Dict[str, Any]],
    ):
        """Get an instance of a Operational Monitoring Dashboard."""
        self.dimensions = defn[0].get("dimensions", {})
        self.xaxis = defn[0]["xaxis"]
        self.compact_visualization = defn[0].get("compact_visualization", False)
        self.group_by_dimension = defn[0].get("group_by_dimension", None)

        super().__init__(title, name, layout, namespace, defn)

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, defn: dict
    ) -> OperationalMonitoringDashboard:
        """Get a OperationalMonitoringDashboard from a dict representation."""
        title = defn["title"]
        return klass(title, name, "newspaper", namespace, defn["tables"])

    def _map_series_to_colours(self, branches, explore):
        colours = [
            "#3FE1B0",
            "#0060E0",
            "#9059FF",
            "#B933E1",
            "#FF2A8A",
            "#FF505F",
            "#FF7139",
            "#FFA537",
            "#005E5D",
            "#073072",
            "#7F165B",
            "#A7341F",
        ]
        return {branch: color for branch, color in zip(branches, colours)}

    def to_lookml(self, bq_client):
        """Get this dashboard as LookML."""
        kwargs = {
            "name": self.name,
            "title": self.title,
            "layout": self.layout,
            "elements": [],
            "dimensions": [],
            "group_by_dimension": self.group_by_dimension,
            "alerts": None,
            "compact_visualization": self.compact_visualization,
        }

        includes = []
        graph_index = 0
        for table_defn in self.tables:
            explore = table_defn["explore"]
            includes.append(
                f"/looker-hub/{self.namespace}/explores/{explore}.explore.lkml"
            )

            if table_defn["table"].endswith("alerts"):
                kwargs["alerts"] = {
                    "explore": explore,
                    "col": 0,
                    "date": (
                        f"{self.xaxis}_date" if self.xaxis == "build_id" else self.xaxis
                    ),
                }
            else:
                if len(kwargs["dimensions"]) == 0:
                    kwargs["dimensions"] = [
                        {
                            "name": name,
                            "title": lookml_utils.slug_to_title(name),
                            "default": info["default"],
                            "options": info["options"],
                        }
                        for name, info in self.dimensions.items()
                    ]

                series_colors = self._map_series_to_colours(
                    table_defn["branches"], explore
                )
                # determine metric groups
                metric_groups = {}
                for summary in table_defn.get("summaries", []):
                    for metric_group in summary.get("metric_groups", []):
                        if metric_group not in metric_groups:
                            metric_groups[metric_group] = [summary["metric"]]
                        elif summary["metric"] not in metric_groups[metric_group]:
                            metric_groups[metric_group].append(summary["metric"])

                seen_metric_groups = []
                for summary in table_defn.get("summaries", []):
                    summary_metric_groups = summary.get("metric_groups", [])
                    if len(summary_metric_groups) == 0:
                        # append a dummy entry if no metric group defined
                        summary_metric_groups.append(None)

                    for metric_group in summary_metric_groups:
                        if (metric_group, summary["statistic"]) in seen_metric_groups:
                            continue

                        if self.compact_visualization:
                            title = "Metric"
                        else:
                            if metric_group is None:
                                title = lookml_utils.slug_to_title(summary["metric"])
                            else:
                                title = lookml_utils.slug_to_title(metric_group)

                        kwargs["elements"].append(
                            {
                                "title": title,
                                "metric": summary["metric"]
                                if metric_group is None
                                else ", ".join(
                                    f'"{m}"' for m in metric_groups[metric_group]
                                ),
                                "statistic": summary["statistic"],
                                "explore": explore,
                                "series_colors": series_colors,
                                "xaxis": self.xaxis,
                                "row": int(graph_index / 2) * 10,
                                "col": 0 if graph_index % 2 == 0 else 12,
                                "is_metric_group": metric_group is not None,
                            }
                        )
                        if metric_group is not None:
                            seen_metric_groups.append(
                                (metric_group, summary["statistic"])
                            )
                        graph_index += 1

                        if self.group_by_dimension:
                            kwargs["elements"].append(
                                {
                                    "title": f"{title} - By {self.group_by_dimension}",
                                    "metric": summary["metric"]
                                    if metric_group is None
                                    else ", ".join(
                                        f'"{m}"' for m in metric_groups[metric_group]
                                    ),
                                    "statistic": summary["statistic"],
                                    "explore": explore,
                                    "series_colors": series_colors,
                                    "xaxis": self.xaxis,
                                    "row": int(graph_index / 2) * 10,
                                    "col": 0 if graph_index % 2 == 0 else 12,
                                    "is_metric_group": metric_group is not None,
                                }
                            )
                            graph_index += 1

                        if self.compact_visualization:
                            # compact visualization only needs a single tile for all probes
                            break

                    if self.compact_visualization:
                        # compact visualization only needs a single tile for all probes
                        break

        if "alerts" in kwargs and kwargs["alerts"] is not None:
            kwargs["alerts"]["row"] = int(graph_index / 2) * 10

        dash_lookml = lookml_utils.render_template(
            "dashboard.lkml", "dashboards", **kwargs
        )
        return dash_lookml
