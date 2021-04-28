"""Generic explore type."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import lkml


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
        raise NotImplementedError("Only implemented in subclasses")

    def get_dependent_views(self) -> List[str]:
        """Get views this explore is dependent on."""
        return [view for _, view in self.views.items()]

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> Explore:
        """Get an instance of an explore from a namespace definition."""
        raise NotImplementedError("Only implemented in subclasses")

    def get_view_lookml(self, view: str) -> dict:
        """Get the LookML for a view."""
        if self.views_path is not None:
            return lkml.load((self.views_path / f"{view}.view.lkml").read_text())
        raise Exception("Missing view path for get_view_lookml")
