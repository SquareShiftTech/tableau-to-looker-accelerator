## Tableau to Looker automated conversion

### How to get oriented
- **From `docs/`**:
  - Review `CODE_ARCHITECTURE.md`
  - Review `ARCHITECTURE_DIAGRAM.svg`
  - Review `tableau_workflow_diagram.svg`

- **From `src/`**:
  - Understand the Tableau XML → JSON pipeline:
    - Parser: `src/tableau_to_looker_parser/core/xml_parser_v2.py`
    - Handlers:
      - Dimensions: `src/tableau_to_looker_parser/handlers/dimension_handler.py`
      - Measures: `src/tableau_to_looker_parser/handlers/measure_handler.py`
      - Calculations: `src/tableau_to_looker_parser/handlers/calculated_field_handler.py`
      - Worksheets: `src/tableau_to_looker_parser/handlers/worksheet_handler.py`
      - Dashboards: `src/tableau_to_looker_parser/handlers/dashboard_handler.py`
    - JSON schema and models: `src/tableau_to_looker_parser/models/json_schema.py` (and related models under `models/`)

  - Understand the JSON → LookML pipeline (generators):
    - Views: `src/tableau_to_looker_parser/generators/view_generator.py`
    - Models: `src/tableau_to_looker_parser/generators/model_generator.py`
    - Dashboards: `src/tableau_to_looker_parser/generators/dashboard_generator.py`
    - Additional utilities and templates under `src/tableau_to_looker_parser/generators/` and `src/tableau_to_looker_parser/templates/`

### Planned documentation and implementation work
- **Dashboard generator improvements**: The current dashboard generator implementation likely needs revision or replacement for clarity and maintainability.
- **Element-driven dashboards**: Aim to generate dashboards with explicit elements using a cleaner, well-defined design plan.

### Implementation direction for dashboards
- Use Looker native charts; ECharts is not required.
- Rebuild the dashboard generation to follow Looker-native visualization patterns.
- Use the existing JSON output as the source of truth for dashboard construction.
- Field names do not need to match Tableau; they should match the names produced by the view generator.
- Keep the overall visualization patterns and layout consistent with the Tableau reference.

- **Reference**: `connected_devices_dashboard/` contains the Tableau workbook, a dashboard PDF, and LookML for the Connected Device Details dashboard. Replicate the structure and layout accordingly.

- **Worksheets mapping**:
  - For "Total Sales by Hour": follow the pattern used by the Connected Device Details in Tableau.
  - Apply the same approach for the remaining worksheets until completion.
