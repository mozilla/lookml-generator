"""Class to describe a Growth Accounting View."""
from __future__ import annotations

from copy import deepcopy
from itertools import filterfalse
from typing import Any, Dict, Iterator, List, Optional, Union

from . import lookml_utils
from .view import View, ViewDict


class GrowthAccountingView(View):
    """A view for growth accounting measures."""

    type: str = "growth_accounting_view"

    other_dimensions: List[Dict[str, str]] = [
        {
            "name": "first",
            "sql": "{TABLE}.first",
            "type": "yesno",
            "hidden": "yes",
        }
    ]

    default_measures: List[Dict[str, Union[str, List[Dict[str, str]]]]] = [
        {
            "name": "overall_active_previous",
            "type": "count",
            "filters": [{"active_last_week": "yes"}],
        },
        {
            "name": "overall_active_current",
            "type": "count",
            "filters": [{"active_this_week": "yes"}],
        },
        {
            "name": "overall_resurrected",
            "type": "count",
            "filters": [
                {"new_last_week": "no"},
                {"new_this_week": "no"},
                {"active_last_week": "no"},
                {"active_this_week": "yes"},
            ],
        },
        {
            "name": "new_users",
            "type": "count",
            "filters": [{"new_this_week": "yes"}, {"active_this_week": "yes"}],
        },
        {
            "name": "established_users_returning",
            "type": "count",
            "filters": [
                {"new_last_week": "no"},
                {"new_this_week": "no"},
                {"active_last_week": "yes"},
                {"active_this_week": "yes"},
            ],
        },
        {
            "name": "new_users_returning",
            "type": "count",
            "filters": [
                {"new_last_week": "yes"},
                {"active_last_week": "yes"},
                {"active_this_week": "yes"},
            ],
        },
        {
            "name": "new_users_churned_count",
            "type": "count",
            "filters": [
                {"new_last_week": "yes"},
                {"active_last_week": "yes"},
                {"active_this_week": "no"},
            ],
        },
        {
            "name": "established_users_churned_count",
            "type": "count",
            "filters": [
                {"new_last_week": "no"},
                {"new_this_week": "no"},
                {"active_last_week": "yes"},
                {"active_this_week": "no"},
            ],
        },
        {
            "name": "new_users_churned",
            "type": "number",
            "sql": "-1 * ${new_users_churned_count}",
        },
        {
            "name": "established_users_churned",
            "type": "number",
            "sql": "-1 * ${established_users_churned_count}",
        },
        {
            "name": "overall_churned",
            "type": "number",
            "sql": "${new_users_churned} + ${established_users_churned}",
        },
        {
            "name": "overall_retention_rate",
            "type": "number",
            "sql": (
                "SAFE_DIVIDE("
                "(${established_users_returning} + ${new_users_returning}),"
                "${overall_active_previous}"
                ")"
            ),
        },
        {
            "name": "established_user_retention_rate",
            "type": "number",
            "sql": (
                "SAFE_DIVIDE("
                "${established_users_returning},"
                "(${established_users_returning} + ${established_users_churned_count})"
                ")"
            ),
        },
        {
            "name": "new_user_retention_rate",
            "type": "number",
            "sql": (
                "SAFE_DIVIDE("
                "${new_users_returning},"
                "(${new_users_returning} + ${new_users_churned_count})"
                ")"
            ),
        },
        {
            "name": "overall_churn_rate",
            "type": "number",
            "sql": (
                "SAFE_DIVIDE("
                "(${established_users_churned_count} + ${new_users_churned_count}),"
                "${overall_active_previous}"
                ")"
            ),
        },
        {
            "name": "fraction_of_active_resurrected",
            "type": "number",
            "sql": "SAFE_DIVIDE(${overall_resurrected}, ${overall_active_current})",
        },
        {
            "name": "fraction_of_active_new",
            "type": "number",
            "sql": "SAFE_DIVIDE(${new_users}, ${overall_active_current})",
        },
        {
            "name": "fraction_of_active_established_returning",
            "type": "number",
            "sql": (
                "SAFE_DIVIDE("
                "${established_users_returning},"
                "${overall_active_current}"
                ")"
            ),
        },
        {
            "name": "fraction_of_active_new_returning",
            "type": "number",
            "sql": "SAFE_DIVIDE(${new_users_returning}, ${overall_active_current})",
        },
        {
            "name": "quick_ratio",
            "type": "number",
            "sql": (
                "SAFE_DIVIDE("
                "${new_users} + ${overall_resurrected},"
                "${established_users_churned_count} + ${new_users_churned_count}"
                ")"
            ),
        },
    ]

    def __init__(self, namespace: str, tables: List[Dict[str, str]], primary_key_field: str = "client_id"):
        """Get an instance of a GrowthAccountingView."""
        self.primary_key_field = primary_key_field

        super().__init__(
            namespace, "growth_accounting", GrowthAccountingView.type, tables
        )

    @classmethod
    def _get_default_dimensions(klass, primary_key_field: str) -> List[Dict[str, str]]:
        return [
            {
                "name": "active_this_week",
                "sql": "mozfun.bits28.active_in_range(days_seen_bits, -6, 7)",
                "type": "yesno",
                "hidden": "yes",
            },
            {
                "name": "active_last_week",
                "sql": "mozfun.bits28.active_in_range(days_seen_bits, -13, 7)",
                "type": "yesno",
                "hidden": "yes",
            },
            {
                "name": "new_this_week",
                "sql": "DATE_DIFF(${submission_date}, first_run_date, DAY) BETWEEN 0 AND 6",
                "type": "yesno",
                "hidden": "yes",
            },
            {
                "name": "new_last_week",
                "sql": "DATE_DIFF(${submission_date}, first_run_date, DAY) BETWEEN 7 AND 13",
                "type": "yesno",
                "hidden": "yes",
            },
            {
                "name": f"{primary_key_field}_day",
                "sql": f"CONCAT(CAST(${{TABLE}}.submission_date AS STRING), ${{{primary_key_field}}})",
                "type": "string",
                "hidden": "yes",
                "primary_key": "yes",
            },
        ]

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
        primary_key_field: str = "client_id",
    ) -> Iterator[GrowthAccountingView]:
        """Get Growth Accounting Views from db views and app variants."""
        dataset = next(
            (channel for channel in channels if channel.get("channel") == "release"),
            channels[0],
        )["dataset"]

        for view_id, references in db_views[dataset].items():
            if view_id == "baseline_clients_last_seen":
                yield GrowthAccountingView(
                    namespace, [{"table": f"mozdata.{dataset}.{view_id}"}], primary_key_field=primary_key_field
                )

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, _dict: ViewDict
    ) -> GrowthAccountingView:
        """Get a view from a name and dict definition."""
        return GrowthAccountingView(namespace, _dict["tables"], primary_key_field=_dict.get("primary_key_field", "client_id"))

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Generate LookML for this view."""
        view_defn: Dict[str, Any] = {"name": self.name}
        table = self.tables[0]["table"]

        # add dimensions and dimension groups
        dimensions = lookml_utils._generate_dimensions(bq_client, table) + deepcopy(
            GrowthAccountingView._get_default_dimensions(primary_key_field=self.primary_key_field)
        )

        view_defn["dimensions"] = list(
            filterfalse(lookml_utils._is_dimension_group, dimensions)
        )
        view_defn["dimension_groups"] = list(
            filter(lookml_utils._is_dimension_group, dimensions)
        )

        # add measures
        view_defn["measures"] = self.get_measures()

        # SQL Table Name
        view_defn["sql_table_name"] = f"`{table}`"

        return {"views": [view_defn]}

    def get_measures(self) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Generate measures for the Growth Accounting Framework."""
        return deepcopy(GrowthAccountingView.default_measures)
