connection: "telemetry"

include: "dashboards/*.dashboard"

{% for include in includes %}
include: "{{include}}"
{% endfor %}
