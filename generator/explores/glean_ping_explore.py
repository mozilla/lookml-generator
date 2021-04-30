from .ping_explore import PingExplore


class GleanPingExplore(PingExplore):
    """A Glean Ping Table explore."""

    type: str = "glean_ping_explore"
