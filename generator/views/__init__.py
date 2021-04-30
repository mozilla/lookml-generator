"""All available Looker views."""
from .growth_accounting_view import GrowthAccountingView
from .ping_view import PingView
from .view import View, ViewDict  # noqa: F401

VIEW_TYPES = {
    PingView.type: PingView,
    GrowthAccountingView.type: GrowthAccountingView,
}
