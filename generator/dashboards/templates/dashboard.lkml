- dashboard: {{name}}
  title: {{title}}
  layout: {{layout}}
  preferred_viewer: dashboards-next

  elements:
  {% for element in elements -%}
  - title: {{element.title}}
    name: {{element.title}}
    explore: {{element.explore}}
    type: "looker_line"
    fields: [
      {{element.explore}}.{{element.xaxis}},
      {{element.explore}}.branch,
      {{element.explore}}.high,
      {{element.explore}}.low,
      {{element.explore}}.percentile
    ]
    pivots: [{{element.explore}}.branch]
    filters:
      {{element.explore}}.probe: {{element.metric}}
    row: {{element.row}}
    col: {{element.col}}
    width: 12
    height: 8
    listen:
      Percentile: {{element.explore}}.percentile_conf
      {%- for dimension in dimensions %}
      {{dimension.title}}: {{element.explore}}.{{dimension.name}}
      {%- endfor %}
    y_axes: [{type: log}]
    series_colors:
      {% for label, colour in element.series_colors.items() -%}
      {{label}}: "{{colour}}"
      {% endfor %}
  {% endfor -%}
  filters:
  - name: Percentile
    title: Percentile
    type: number_filter
    default_value: '50'
    allow_multiple_values: false
    required: true
    ui_config:
      type: dropdown_menu
      display: inline
      options:
      - '10'
      - '20'
      - '30'
      - '40'
      - '50'
      - '60'
      - '70'
      - '80'
      - '90'
      - '95'
      - '99'

  {% for dimension in dimensions -%}
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
  {% endfor -%}
