"""Class to describe a Funnel Analysis View."""
from __future__ import annotations

from typing import Dict, Iterator, List

from .view import View


class FunnelAnalysisView(View):
    """A view for Client Counting measures."""

    type: str = "funnel_analysis_view"

    def __init__(self, namespace: str, tables: List[Dict[str, str]]):
        """Get an instance of a FunnelAnalysisView."""
        super().__init__(namespace, "funnel_analysis", FunnelAnalysisView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[FunnelAnalysisView]:
        """Get Client Count Views from db views and app variants."""
        # We can guarantee there will always be at least one channel,
        # because this comes from the associated _get_glean_repos in
        # namespaces.py
        dataset = next(
            (channel for channel in channels if channel.get("channel") == "release"),
            channels[0],
        )["dataset"]

        necessary_views = {"events_daily", "event_types"}
        for view_id, references in db_views[dataset].items():
            necessary_views -= {view_id}

        if len(necessary_views) == 0:
            yield FunnelAnalysisView(
                namespace,
                [
                    {
                        "events_daily_view": "events_daily_table",
                        "event_types_view": "event_types_table",
                    },
                ],
            )
