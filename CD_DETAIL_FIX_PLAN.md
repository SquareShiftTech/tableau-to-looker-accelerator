# CD Detail Field Mapping Fix Implementation Plan

## Problem Summary

The "CD detail" dashboard element (corresponding to Tableau's "Total Sales by Hour") generates incorrect field mappings, pivots, and sorts compared to the manually created version. The core issues are:

1. **Wrong field naming**: `DAY(RPT_DT)` should map to `day_rpt_dt`, not `rpt_dt_date`
2. **Too many fields**: Including 17+ fields instead of only the 4 relevant fields
3. **Wrong pivots**: Not using columns shelf fields as pivots
4. **Wrong chart type**: Not detecting table visualization for pivot scenarios

## Data Analysis

### JSON Data Structure (CD Detail Worksheet)
```json
{
  "name": "CD Detail",
  "x_axis": ["tdy:RPT_DT:ok", "thr:RPT_TIME:ok"],  // Columns shelf
  "y_axis": ["none:EQP_GRP_DESC:nk"],              // Rows shelf
  "color": "sum:sales",                            // Measure
  "chart_type": "table"
}
```

### Expected Output (Reference: Total Sales by Hour)
```yaml
fields: [day_rpt_dt, hour_rpt_time, eqp_grp_desc, sum_sales]
pivots: [day_rpt_dt, hour_rpt_time]
sorts: [day_rpt_dt, hour_rpt_time]
type: table
```

### Current Wrong Output (CD Detail)
```yaml
fields: [17+ fields including calculated fields]
pivots: [eqp_grp_desc]  # Wrong - should be columns shelf fields
sorts: [all 17+ fields]
type: echarts_visualization_prod  # Wrong - should be table
```

## Implementation Steps

### Step 1: Fix Field Mapping Logic
**File**: `src/tableau_to_looker_parser/generators/utils/field_mapping.py`
**Method**: `_add_measure_aggregation_type()`

**Current Issue**:
- `DAY(RPT_DT)` maps to `rpt_dt_date`
- `HOUR(RPT_TIME)` maps to wrong field name

**Fix**:
- Map `tdy:RPT_DT:ok` → `day_rpt_dt` (using dimension group timeframe)
- Map `thr:RPT_TIME:ok` → `hour_rpt_time` (using dimension group timeframe)
- Use proper timeframe prefixes based on Tableau function type

### Step 2: Fix Field Selection Logic
**File**: `src/tableau_to_looker_parser/generators/dashboard_generator.py`
**Method**: `_build_dashboard_element_from_worksheet()`

**Current Issue**:
- Including all worksheet fields instead of filtering to shelf-relevant fields
- Adding calculated fields that aren't used in the visualization

**Fix**:
- Only include fields from x_axis (columns shelf)
- Only include fields from y_axis (rows shelf)
- Only include the color/measure field
- Exclude calculated fields not used in shelves
- Result: 4 fields instead of 17+

### Step 3: Fix Pivot Logic
**File**: `src/tableau_to_looker_parser/generators/dashboard_generator.py`
**Method**: `_build_pivots_from_worksheet()`

**Current Issue**:
- Using y_axis fields as pivots
- Not using x_axis (columns shelf) fields as pivots

**Fix**:
- Use x_axis fields as pivots: `["day_rpt_dt", "hour_rpt_time"]`
- Keep y_axis fields as regular dimensions in rows
- Pivot logic: x_axis fields become LookML pivots

### Step 4: Fix Chart Type Detection
**File**: `src/tableau_to_looker_parser/converters/enhanced_chart_type_detector.py`
**Method**: `detect_chart_type()`

**Current Issue**:
- Not detecting table visualization when pivots are present
- Defaulting to echarts_visualization_prod

**Fix**:
- When pivots exist (x_axis has fields), detect as table
- Chart type should be "table" for pivot scenarios
- Override echarts detection for pivot-based visualizations

## Implementation Priority

1. **High Priority**: Steps 1-3 (field mapping, selection, pivots) - core functionality
2. **Medium Priority**: Step 4 (chart type) - visualization improvement
3. **Low Priority**: Testing and validation

## Validation Criteria

After implementation, CD Detail should generate:
- **Fields**: `[day_rpt_dt, hour_rpt_time, eqp_grp_desc, sum_sales]` (4 fields)
- **Pivots**: `[day_rpt_dt, hour_rpt_time]` (from x_axis)
- **Chart Type**: `table` (not echarts)
- **Sorts**: Include pivot fields in sort order

## File Locations

- **Field Mapping**: `src/tableau_to_looker_parser/generators/utils/field_mapping.py`
- **Dashboard Generator**: `src/tableau_to_looker_parser/generators/dashboard_generator.py`
- **Chart Type Detector**: `src/tableau_to_looker_parser/converters/enhanced_chart_type_detector.py`
- **View Template**: `src/tableau_to_looker_parser/templates/basic_view.j2`

## Key Data Sources

- **JSON Input**: `comprehensive_dashboard_test_output/processed_pipeline_output.json`
- **Reference Manual**: `connected_devices_dashboard/Intraday_Sales.dashboard.lookml`
- **Current Wrong Output**: `comprehensive_dashboard_test_output/connected_devices_detail.dashboard`
- **View File**: `comprehensive_dashboard_test_output/intradaysales_results_hqa_pd_qmtbls_mock.view.lkml`
