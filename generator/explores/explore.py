"""Generic explore type."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Explore:
    """A generic explore."""

    name: str
    views: Dict[str, str]
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
    def from_dict(name: str, defn: dict) -> Explore:
        """Get an instance of an explore from a namespace definition."""
        raise NotImplementedError("Only implemented in subclasses")
