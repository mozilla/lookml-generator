"""Class to describe a Client Counts View."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterator, List, Optional, Union

from .view import View, ViewDict


class ClientCountsView(View):
    """A view for Client Counting measures."""

    type: str = "client_counts_view"

    default_dimension_groups: List[Dict[str, Union[str, List[str]]]] = [
        {
            "name": "since_first_seen",
            "type": "duration",
            "description": "Amount of time that has passed since the client was first seen.",
            "sql_start": "CAST(${TABLE}.first_seen_date AS TIMESTAMP)",
            "sql_end": "CAST(${TABLE}.submission_date AS TIMESTAMP)",
            "intervals": ["day", "week", "month", "year"],
        }
    ]

    default_dimensions: List[Dict[str, str]] = [
        {
            "name": "have_completed_period",
            "type": "yesno",
            "description": "Only for use with cohort analysis."
            "Filter on true to remove the tail of incomplete data from cohorts."
            "Indicates whether the cohort for this row have all had a chance to complete this interval."
            "For example, new clients from yesterday have not all had a chance to send a ping for today.",
            "sql": """
              DATE_ADD(
                {% if client_counts.first_seen_date._is_selected %}
                  DATE_ADD(DATE(${client_counts.first_seen_date}), INTERVAL 1 DAY)
                {% elsif client_counts.first_seen_week._is_selected %}
                  DATE_ADD(DATE(${client_counts.first_seen_week}), INTERVAL 1 WEEK)
                {% elsif client_counts.first_seen_month._is_selected %}
                  DATE_ADD(PARSE_DATE('%Y-%m', ${client_counts.first_seen_month}), INTERVAL 1 MONTH)
                {% elsif client_counts.first_seen_year._is_selected %}
                  DATE_ADD(DATE(${client_counts.first_seen_year}, 1, 1), INTERVAL 1 YEAR)
                {% endif %}
                ,
                {% if client_counts.days_since_first_seen._is_selected %}
                  INTERVAL ${client_counts.days_since_first_seen} DAY
                {% elsif client_counts.weeks_since_first_seen._is_selected %}
                  INTERVAL ${client_counts.weeks_since_first_seen} WEEK
                {% elsif client_counts.months_since_first_seen._is_selected %}
                  INTERVAL ${client_counts.months_since_first_seen} MONTH
                {% elsif client_counts.years_since_first_seen._is_selected %}
                  INTERVAL ${client_counts.months_since_first_seen} YEAR
                {% endif %}
              ) < current_date
              """,
        }
    ]

    default_measures: List[Dict[str, Union[str, List[Dict[str, str]]]]] = [
        {
            "name": "client_count",
            "type": "number",
            "description": "The number of clients, "
            "determined by whether they sent a baseline ping on the day in question.",
            "sql": "COUNT(DISTINCT client_id)",
        }
    ]

    def __init__(self, namespace: str, tables: List[Dict[str, str]]):
        """Get an instance of a ClientCountsView."""
        super().__init__(namespace, "client_counts", ClientCountsView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[ClientCountsView]:
        """Get Client Count Views from db views and app variants."""
        # We can guarantee there will always be at least one channel,
        # because this comes from the associated _get_glean_repos in
        # namespaces.py
        dataset = next(
            (channel for channel in channels if channel.get("channel") == "release"),
            channels[0],
        )["dataset"]

        for view_id, references in db_views[dataset].items():
            if view_id == "baseline_clients_daily" or view_id == "clients_daily":
                yield ClientCountsView(
                    namespace, [{"table": f"mozdata.{dataset}.{view_id}"}]
                )

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, _dict: ViewDict
    ) -> ClientCountsView:
        """Get a view from a name and dict definition."""
        return ClientCountsView(namespace, _dict["tables"])

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Generate LookML for this view."""
        table = self.tables[0]["table"]

        base_view = "baseline_clients_daily_table"
        if table is not None:
            base_view = table.split(".")[-1] + "_table"

        view_defn: Dict[str, Any] = {
            "extends": [base_view],
            "name": self.name,
        }

        # add dimensions and dimension groups
        view_defn["dimensions"] = deepcopy(ClientCountsView.default_dimensions)
        view_defn["dimension_groups"] = deepcopy(
            ClientCountsView.default_dimension_groups
        )

        # add measures
        view_defn["measures"] = self.get_measures()

        return {
            "includes": [base_view + ".view.lkml"],
            "views": [view_defn],
        }

    def get_measures(self) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Generate measures for the Growth Accounting Framework."""
        return deepcopy(ClientCountsView.default_measures)
