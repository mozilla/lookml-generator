"""All possible explore types."""
from .explore import Explore  # noqa: F401 isort:skip
from .client_counts_explore import ClientCountsExplore
from .events_explore import EventsExplore
from .funnel_analysis_explore import FunnelAnalysisExplore
from .glean_ping_explore import GleanPingExplore
from .growth_accounting_explore import GrowthAccountingExplore
from .operational_monitoring_explore import OperationalMonitoringExplore
from .ping_explore import PingExplore

EXPLORE_TYPES = {
    ClientCountsExplore.type: ClientCountsExplore,
    EventsExplore.type: EventsExplore,
    FunnelAnalysisExplore.type: FunnelAnalysisExplore,
    GleanPingExplore.type: GleanPingExplore,
    PingExplore.type: PingExplore,
    GrowthAccountingExplore.type: GrowthAccountingExplore,
    OperationalMonitoringExplore.type: OperationalMonitoringExplore,
}
