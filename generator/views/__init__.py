"""All available Looker views."""
from .client_counts_view import ClientCountsView
from .funnel_analysis_view import FunnelAnalysisView
from .glean_ping_view import GleanPingView

# from .glean_unnested_label_view import GleanUnnestedLabelView
from .growth_accounting_view import GrowthAccountingView
from .ping_view import PingView
from .table_view import TableView
from .view import View, ViewDict  # noqa: F401

VIEW_TYPES = {
    ClientCountsView.type: ClientCountsView,
    FunnelAnalysisView.type: FunnelAnalysisView,
    GleanPingView.type: GleanPingView,
    # GleanUnnestedLabelView.type: GleanUnnestedLabelView,
    PingView.type: PingView,
    GrowthAccountingView.type: GrowthAccountingView,
    TableView.type: TableView,
}
