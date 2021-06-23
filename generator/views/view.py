"""Generic class to describe Looker views."""
from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, TypedDict

from click import ClickException

OMIT_VIEWS = {"deletion_request"}


class ViewDict(TypedDict):
    """Represent a view definition."""

    type: str
    tables: List[Dict[str, str]]


class View(object):
    """A generic Looker View."""

    name: str
    view_type: str
    tables: List[Dict[str, str]]
    namespace: str

    def __init__(
        self,
        namespace: str,
        name: str,
        view_type: str,
        tables: List[Dict[str, str]],
        **kwargs,
    ):
        """Create an instance of a view."""
        self.namespace = namespace
        self.tables = tables
        self.name = name
        self.view_type = view_type

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[View]:
        """Get Looker views from app."""
        raise NotImplementedError("Only implemented in subclass.")

    @classmethod
    def from_dict(klass, namespace: str, name: str, _dict: ViewDict) -> View:
        """Get a view from a name and dict definition."""
        raise NotImplementedError("Only implemented in subclass.")

    def get_type(self) -> str:
        """Get the type of this view."""
        return self.view_type

    def as_dict(self) -> dict:
        """Get this view as a dictionary."""
        return {
            "type": self.view_type,
            "tables": self.tables,
        }

    def __str__(self):
        """Stringify."""
        return f"name: {self.name}, type: {self.type}, table: {self.tables}, namespace: {self.namespace}"

    def __eq__(self, other) -> bool:
        """Check for equality with other View."""

        def comparable_dict(d):
            return {tuple(sorted(t.items())) for t in d}

        if isinstance(other, View):
            return (
                self.name == other.name
                and self.view_type == other.view_type
                and comparable_dict(self.tables) == comparable_dict(other.tables)
                and self.namespace == other.namespace
            )
        return False

    def get_dimensions(
        self, bq_client, table, v1_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Get the set of dimensions for this view."""
        raise NotImplementedError("Only implemented in subclass.")

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """
        Generate Lookml for this view.

        View instances can generate more than one Looker view,
        for e.g. nested fields and joins, so this returns
        a list.
        """
        raise NotImplementedError("Only implemented in subclass.")

    def get_client_id(self, dimensions: List[dict], table: str) -> Optional[str]:
        """Return the first field that looks like a client identifier."""
        client_id_fields = [
            d["name"]
            for d in dimensions
            if d["name"] in {"client_id", "client_info__client_id", "context_id"}
        ]
        if not client_id_fields:
            # Some pings purposely disinclude client_ids, e.g. firefox installer
            return None
        if len(client_id_fields) > 1:
            raise ClickException(f"Duplicate client_id dimension in {table!r}")
        return client_id_fields[0]
