- dashboard: {{name}}
  title: {{title}}
  layout: {{layout}}
  preferred_viewer: dashboards-next

  elements:
  {% for element in elements -%}
  - title: {{element.title}}
    name: {{element.title}}_{{element.statistic}}
    note_state: expanded
    note_display: above
    note_text: {{element.statistic.title()}}
    explore: {{element.explore}}
    {% if element.statistic == "percentile" -%}
    type: "ci-line-chart"
    {% else -%}
    type: looker_line
    {% endif -%}
    fields: [
      {{element.explore}}.{{element.xaxis}},
      {{element.explore}}.branch,
      {% if element.statistic == "percentile" -%}
      {{element.explore}}.upper,
      {{element.explore}}.lower,
      {% endif -%}
      {{element.explore}}.point
    ]
    pivots: [
      {{element.explore}}.branch 
      {%- if group_by_dimension and element.title.endswith(group_by_dimension) %}, {{element.explore}}.{{group_by_dimension}} {% endif %}
    ]
    {% if not compact_visualization -%}
    filters:
      {{element.explore}}.metric: {{element.metric}}
      {{element.explore}}.statistic: {{element.statistic}}
    {% endif -%}
    row: {{element.row}}
    col: {{element.col}}
    width: 12
    height: 8
    field_x: {{element.explore}}.{{element.xaxis}}
    field_y: {{element.explore}}.point
    log_scale: false
    ci_lower: {{element.explore}}.lower
    ci_upper: {{element.explore}}.upper
    show_grid: true
    listen:
      {%- if element.statistic == "percentile" %}
      Percentile: {{element.explore}}.parameter
      {%- endif %}
      {%- for dimension in dimensions %}
      {{dimension.title}}: {{element.explore}}.{{dimension.name}}
      {%- endfor %}
      {% if compact_visualization -%}
      Metric: {{element.explore}}.metric
      {% endif -%}
    {%- for branch, color in element.series_colors.items() %}
    {{ branch }}: "{{ color }}"
    {%- endfor %}
    defaults_version: 0
  {% endfor -%}
  {% if alerts is not none %}
  - title: Alerts
    name: Alerts
    model: operational_monitoring
    explore: {{alerts.explore}}
    type: looker_grid
    fields: [{{alerts.explore}}.submission_date,
      {{alerts.explore}}.metric, {{alerts.explore}}.statistic, {{alerts.explore}}.percentile,
      {{alerts.explore}}.message, {{alerts.explore}}.branch, {{alerts.explore}}.errors]
    sorts: [{{alerts.explore}}.submission_date
        desc]
    limit: 500
    show_view_names: false
    show_row_numbers: true
    transpose: false
    truncate_text: true
    hide_totals: false
    hide_row_totals: false
    size_to_fit: true
    table_theme: white
    limit_displayed_rows: false
    enable_conditional_formatting: false
    header_text_alignment: left
    header_font_size: 12
    rows_font_size: 12
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    show_null_points: true
    interpolation: linear
    defaults_version: 1
    series_types: {}
    listen: {}
    row: {{ alerts.row }}
    col: {{ alerts.col }}
    width: 24
    height: 6
  {% endif %}
  filters:
  - name: Percentile
    title: Percentile
    type: field_filter
    default_value: '50'
    allow_multiple_values: false
    required: true
    ui_config:
      type: slider
      display: inline
      options: []
    model: operational_monitoring
    explore: {{ elements[0].explore }}
    listens_to_filters: []
    field: {{ elements[0].explore }}.parameter
  {% if compact_visualization -%}
  - name: Metric
    title: Metric
    type: field_filter
    default_value: '{{ elements[0].metric }}'
    allow_multiple_values: false
    required: true
    ui_config:
      type: dropdown_menu
      display: popover
    model: operational_monitoring
    explore: {{ elements[0].explore }}
    listens_to_filters: []
    field: {{ elements[0].explore }}.metric
  - name: Statistic
    title: Statistic
    type: field_filter
    default_value: '{{ elements[0].statistic }}'
    allow_multiple_values: false
    required: true
    ui_config:
      type: dropdown_menu
      display: popover
    model: operational_monitoring
    explore: {{ elements[0].explore }}
    listens_to_filters: []
    field: {{ elements[0].explore }}.statistic
  {% endif -%}

  {% for dimension in dimensions -%}
  {% if dimension.name != group_by_dimension %}
  - title: {{dimension.title}}
    name: {{dimension.title}}
    type: string_filter
    default_value: '{{dimension.default}}'
    allow_multiple_values: false
    required: true
    ui_config:
      type: dropdown_menu
      display: inline
      options:
      {% for option in dimension.options -%}
      - '{{option}}'
      {% endfor %}
  {% else %}
  - title: {{dimension.title}}
    name: {{dimension.title}}
    type: string_filter
    default_value: '{% for option in dimension.options | sort -%}{{option}}{% if not loop.last %},{% endif %}{% endfor %}'
    allow_multiple_values: true
    required: true
    ui_config:
      type: advanced
      display: inline
      options:
      {% for option in dimension.options | sort -%}
      - '{{option}}'
      {% endfor %}
  {% endif %}
  {% endfor -%}
