"""All possible explore types."""
from .explore import Explore  # noqa: F401
from .glean_ping_explore import GleanPingExplore
from .growth_accounting_explore import GrowthAccountingExplore
from .ping_explore import PingExplore

EXPLORE_TYPES = {
    GleanPingExplore.type: GleanPingExplore,
    PingExplore.type: PingExplore,
    GrowthAccountingExplore.type: GrowthAccountingExplore,
}

GLEAN_EXPLORE_TYPES = {
    t: v
    for (t, v) in EXPLORE_TYPES.items()
    if t in [GleanPingExplore.type, GrowthAccountingExplore.type]
}
