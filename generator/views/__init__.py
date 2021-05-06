"""All available Looker views."""
from .client_count_view import ClientCountView
from .glean_ping_view import GleanPingView
from .growth_accounting_view import GrowthAccountingView
from .ping_view import PingView
from .table_view import TableView
from .view import View, ViewDict  # noqa: F401

VIEW_TYPES = {
    ClientCountView.type: ClientCountView,
    GleanPingView.type: GleanPingView,
    PingView.type: PingView,
    GrowthAccountingView.type: GrowthAccountingView,
    TableView.type: TableView,
}
