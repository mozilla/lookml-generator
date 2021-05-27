"""Views representing labeled unnested views."""
from .glean_ping_view import GleanPingView


class GleanUnnestedLabelView(GleanPingView):
    """A view on an unnested field in a Glean ping view."""

    type: str = "glean_unnested_label_view"
