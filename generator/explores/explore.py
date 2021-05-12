"""Generic explore type."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import lkml

from ..views.lookml_utils import escape_filter_expr


@dataclass
class Explore:
    """A generic explore."""

    name: str
    views: Dict[str, str]
    views_path: Optional[Path] = None
    type: str = field(init=False)

    def to_dict(self) -> dict:
        """Explore instance represented as a dict."""
        return {self.name: {"type": self.type, "views": self.views}}

    def to_lookml(self) -> dict:
        """Generate LookML for this explore."""
        base_lookml = {}
        for view_type, view in self.views.items():
            if "join" in view_type:
                continue
            if self._get_view_has_submission(view):
                base_lookml[
                    "sql_always_where"
                ] = f"${{{self.name}.submission_date}} >= '2010-01-01'"

        base_lookml.update(self._to_lookml())
        return base_lookml

    def _to_lookml(self) -> dict:
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
