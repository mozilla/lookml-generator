"""Generic dashboard type."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Dashboard(object):
    """A generic Looker Dashboard."""

    title: str
    name: str
    layout: str
    namespace: str
    tables: List[Dict[str, str]]
    type: str = field(init=False)

    def to_dict(self) -> dict:
        """Dashboard instance represented as a dict."""
        return {
            self.name: {
                "title": self.title,
                "type": self.type,
                "layout": self.layout,
                "namespace": self.namespace,
                "tables": self.tables,
            }
        }

    def to_lookml(self, client):
        """Generate Lookml for this dashboard."""
        raise NotImplementedError("Only implemented in subclass.")
