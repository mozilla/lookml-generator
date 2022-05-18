"""All available Looker views."""
from .client_counts_view import ClientCountsView
from .events_view import EventsView
from .funnel_analysis_view import FunnelAnalysisView
from .glean_ping_view import GleanPingView
from .growth_accounting_view import GrowthAccountingView
from .operational_monitoring_alerting_view import OperationalMonitoringAlertingView
from .operational_monitoring_histogram_view import OperationalMonitoringHistogramView
from .operational_monitoring_scalar_view import OperationalMonitoringScalarView
from .ping_view import PingView
from .table_view import TableView
from .view import View, ViewDict  # noqa: F401

VIEW_TYPES = {
    ClientCountsView.type: ClientCountsView,
    EventsView.type: EventsView,
    FunnelAnalysisView.type: FunnelAnalysisView,
    GleanPingView.type: GleanPingView,
    PingView.type: PingView,
    GrowthAccountingView.type: GrowthAccountingView,
    OperationalMonitoringScalarView.type: OperationalMonitoringScalarView,
    OperationalMonitoringHistogramView.type: OperationalMonitoringHistogramView,
    OperationalMonitoringAlertingView.type: OperationalMonitoringAlertingView,
    TableView.type: TableView,
}
