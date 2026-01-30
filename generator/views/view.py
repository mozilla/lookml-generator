"""Generic class to describe Looker views."""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Set, TypedDict

from click import ClickException

OMIT_VIEWS: Set[str] = set()


# TODO: Once we upgrade to Python 3.11 mark just `measures` as non-required, not all keys. -- TODO: need to follow-up on this.
class ViewDict(TypedDict, total=False):
    """Represent a view definition."""

    type: str
    tables: List[Dict[str, str]]
    measures: Dict[str, Dict[str, Any]]


class View(object):
    """A generic Looker View."""

    name: str
    view_type: str
    tables: List[Dict[str, Any]]
    namespace: str

    def __init__(
        self,
        namespace: str,
        name: str,
        view_type: str,
        tables: List[Dict[str, Any]],
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
            return {tuple(sorted([(k, str(v)) for k, v in t.items()])) for t in d}

        if isinstance(other, View):
            return (
                self.name == other.name
                and self.view_type == other.view_type
                and comparable_dict(self.tables) == comparable_dict(other.tables)
                and self.namespace == other.namespace
            )
        return False

    def get_dimensions(
        self, table, v1_name: Optional[str], dryrun
    ) -> List[Dict[str, Any]]:
        """Get the set of dimensions for this view."""
        raise NotImplementedError("Only implemented in subclass.")

    def to_lookml(self, v1_name: Optional[str], dryrun) -> Dict[str, Any]:
        """
        Generate Lookml for this view.

        View instances can generate more than one Looker view,
        for e.g. nested fields and joins, so this returns
        a list.
        """
        raise NotImplementedError("Only implemented in subclass.")

    def get_client_id(self, dimensions: List[dict], table: str) -> Optional[str]:
        """Return the first field that looks like a client identifier."""
        client_id_fields = self.select_dimension(
            {"client_id", "client_info__client_id", "context_id"},
            dimensions,
            table,
        )
        # Some pings purposely disinclude client_ids, e.g. firefox installer
        return client_id_fields["name"] if client_id_fields else None

    def get_document_id(self, dimensions: List[dict], table: str) -> Optional[str]:
        """Return the first field that looks like a document_id."""
        document_id = self.select_dimension("document_id", dimensions, table)
        return document_id["name"] if document_id else None

    def select_dimension(
        self,
        dimension_names: str | set[str],
        dimensions: List[dict],
        table: str,
    ) -> Optional[dict[str, str]]:
        """
        Return the first field that matches dimension name.

        Throws if the query set is greater than one and more than one item is selected.
        """
        if isinstance(dimension_names, str):
            dimension_names = {dimension_names}
        selected = [d for d in dimensions if d["name"] in dimension_names]
        if selected:
            # there should only be one dimension selected from the set
            # if there are multiple options in the dimention_names set.
            if len(dimension_names) > 1 and len(selected) > 1:
                raise ClickException(
                    f"Duplicate {'/'.join(dimension_names)} dimension in {table!r}"
                )
            return selected[0]
        return None
