"""All possible dashboard types."""

from .dashboard import Dashboard  # noqa: F401
from .operational_monitoring_dashboard import OperationalMonitoringDashboard

DASHBOARD_TYPES = {
    OperationalMonitoringDashboard.type: OperationalMonitoringDashboard,
}
