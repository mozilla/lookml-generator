"""Table explore type."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from ..views import TableView, View
from . import Explore

ALLOWED_VIEWS = {"events_stream_table"}


class TableExplore(Explore):
    """A table explore."""

    type: str = "table_explore"

    def _to_lookml(self, v1_name: Optional[str]) -> List[Dict[str, Any]]:
        """Generate LookML to represent this explore."""
        explore_lookml: Dict[str, Any] = {
            "name": self.name,
            "view_name": self.views["base_view"],
            "joins": self.get_unnested_fields_joins_lookml(),
        }
        if required_filters := self.get_required_filters("base_view"):
            explore_lookml["always_filter"] = {
                "filters": required_filters,
            }

        if datagroup := self.get_datagroup():
            explore_lookml["persist_with"] = datagroup

        return [explore_lookml]

    @staticmethod
    def from_views(views: List[View]) -> Iterator[TableExplore]:
        """Don't generate all possible TableExplores from the views, only generate for ALLOWED_VIEWS."""
        for view in views:
            if view.view_type == TableView.type:
                if view.name in ALLOWED_VIEWS:
                    yield TableExplore(view.name, {"base_view": view.name})

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> TableExplore:
        """Get an instance of this explore from a name and dictionary definition."""
        return TableExplore(name, defn["views"], views_path)

    def get_datagroup(self) -> Optional[str]:
        """
        Return the name of the associated datagroup.

        Return `None` if there is no datagroup for this explore.
        """
        datagroups_path = self.views_path.parent / "datagroups"
        if self.views_path and datagroups_path.exists():
            datagroup_file = (
                datagroups_path
                / f'{self.views["base_view"]}_last_updated.datagroup.lkml'
            )
            if datagroup_file.exists():
                return f'{self.views["base_view"]}_last_updated'
        return None
