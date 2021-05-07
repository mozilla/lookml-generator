"""All possible explore types."""
from .explore import Explore  # noqa: F401 isort:skip
from .client_counts_explore import ClientCountsExplore
from .glean_ping_explore import GleanPingExplore
from .growth_accounting_explore import GrowthAccountingExplore
from .ping_explore import PingExplore

EXPLORE_TYPES = {
    ClientCountsExplore.type: ClientCountsExplore,
    GleanPingExplore.type: GleanPingExplore,
    PingExplore.type: PingExplore,
    GrowthAccountingExplore.type: GrowthAccountingExplore,
}
