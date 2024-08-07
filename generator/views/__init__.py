"""All available Looker views."""

from .client_counts_view import ClientCountsView
from .events_view import EventsView
from .funnel_analysis_view import FunnelAnalysisView
from .glean_ping_view import GleanPingView
from .growth_accounting_view import GrowthAccountingView
from .metric_definitions_view import MetricDefinitionsView
from .operational_monitoring_alerting_view import OperationalMonitoringAlertingView
from .operational_monitoring_view import OperationalMonitoringView
from .ping_view import PingView
from .table_view import TableView
from .view import View, ViewDict  # noqa: F401

VIEW_TYPES = {
    ClientCountsView.type: ClientCountsView,
    EventsView.type: EventsView,
    FunnelAnalysisView.type: FunnelAnalysisView,
    OperationalMonitoringView.type: OperationalMonitoringView,
    OperationalMonitoringAlertingView.type: OperationalMonitoringAlertingView,
    MetricDefinitionsView.type: MetricDefinitionsView,
    GleanPingView.type: GleanPingView,
    PingView.type: PingView,
    GrowthAccountingView.type: GrowthAccountingView,
    TableView.type: TableView,
}
