"""All possible explore types."""

from .explore import Explore  # noqa: F401 isort:skip
from .client_counts_explore import ClientCountsExplore
from .events_explore import EventsExplore
from .events_stream_explore import EventsStreamExplore
from .funnel_analysis_explore import FunnelAnalysisExplore
from .glean_ping_explore import GleanPingExplore
from .growth_accounting_explore import GrowthAccountingExplore
from .metric_definitions_explore import MetricDefinitionsExplore
from .operational_monitoring_explore import (
    OperationalMonitoringAlertingExplore,
    OperationalMonitoringExplore,
)
from .ping_explore import PingExplore
from .table_explore import TableExplore

EXPLORE_TYPES = {
    ClientCountsExplore.type: ClientCountsExplore,
    EventsExplore.type: EventsExplore,
    EventsStreamExplore.type: EventsStreamExplore,
    FunnelAnalysisExplore.type: FunnelAnalysisExplore,
    GleanPingExplore.type: GleanPingExplore,
    PingExplore.type: PingExplore,
    GrowthAccountingExplore.type: GrowthAccountingExplore,
    MetricDefinitionsExplore.type: MetricDefinitionsExplore,
    OperationalMonitoringExplore.type: OperationalMonitoringExplore,
    OperationalMonitoringAlertingExplore.type: OperationalMonitoringAlertingExplore,
    TableExplore.type: TableExplore,
}
