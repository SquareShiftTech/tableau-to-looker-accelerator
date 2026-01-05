import os
import re
import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
from google.cloud import bigquery
from utils.Complexity_Analysis import detect_features_from_json, load_json_file
from utils.Executive_Summary import get_dashboards_from_json

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_TABLE = os.getenv("DATASET_TABLE")
WORKBOOK_DIR = Path("output") / "workbook"

def normalize_key(s):
    return re.sub(r'[^a-zA-Z0-9]', '', s).lower()

def get_nested(data, *keys, default=None):
    """Helper function to safely get nested values from JSON data."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
        if data is None:
            return default
    return data

def count_dashboards_from_json(json_data):
    """Count total number of dashboards in JSON data."""
    dashboards = get_nested(json_data, "dashboards", default=[])
    return len(dashboards) if dashboards else 0

def count_worksheets_from_json(json_data):
    """Count total number of worksheets across all dashboards in JSON data."""
    dashboards = get_nested(json_data, "dashboards", default=[])
    total_worksheets = 0
    for dashboard in dashboards:
        worksheets = get_nested(dashboard, "worksheet", default=[])
        total_worksheets += len(worksheets) if worksheets else 0
    return total_worksheets

def count_datasources_from_json(json_data):
    """Count data sources and detect joins from JSON data."""
    connections = get_nested(json_data, "connections", default=[])
    tables = get_nested(json_data, "tables", default=[])
    relationships = get_nested(json_data, "relationships", default=[])
    
    datasource_count = len(connections) if connections else 0
    has_joins = len(relationships) > 0 if relationships else False
    
    return datasource_count, has_joins

def count_dashboard_tiles_from_json(dashboard_json):
    """Count visual tiles in a dashboard from JSON data."""
    try:
        worksheets = get_nested(dashboard_json, "worksheet", default=[])
        visual_tiles = set()
        
        for worksheet in worksheets:
            worksheet_name = get_nested(worksheet, "name", default="")
            if worksheet_name:
                # Each worksheet in a dashboard is considered a visual tile
                visual_tiles.add(worksheet_name)
        
        return len(visual_tiles)
    except Exception as e:
        print(f"Error counting dashboard tiles: {e}")
        return 0

def type_of_lod_present_from_json(json_data):
    """Detect LOD expressions from JSON data."""
    try:
        fixed_lod_present = False
        complex_lod_present = False
        
        # Check calculated fields
        calculated_fields = get_nested(json_data, "calculated_fields", default=[])
        for calc_field in calculated_fields:
            calculation = get_nested(calc_field, "calculation", default={})
            original_formula = get_nested(calculation, "original_formula", default="")
            
            if original_formula:
                cleaned = original_formula.replace(" ", "").upper()
                if cleaned.startswith("{FIXED"):
                    fixed_lod_present = True
                elif cleaned.startswith("{INCLUDE") or cleaned.startswith("{EXCLUDE"):
                    complex_lod_present = True
                
                if fixed_lod_present and complex_lod_present:
                    break
        
        return fixed_lod_present, complex_lod_present
        
    except Exception as e:
        print(f"Error detecting LOD expressions: {e}")
        return False, False

def assess_dashboard_complexity_from_json(dashboard_json_with_context):
    """Assess individual dashboard complexity based on JSON data."""
    try:
        # Extract dashboard and workbook-level data
        dashboards = get_nested(dashboard_json_with_context, "dashboards", default=[])
        if not dashboards:
            return "Low"
        
        dashboard_json = dashboards[0]  # Get the first dashboard
        
        # Detect features from JSON
        features = detect_features_from_json(dashboard_json_with_context)
        
        # Initialize complexity scores for each category
        complexity_scores = {
            'data_sources': 0,
            'worksheets': 0,
            'dashboards': 0,
            'dashboard_features': 0,
            'filters_groups': 0,
            'calculations': 0,
            'actions_interactivity': 0,
            'parameters': 0,
            'charts': 0
        }
        
        # 1. Number of Data Sources
        datasource_count, has_joins = count_datasources_from_json(dashboard_json_with_context)
        if datasource_count == 1 and not features.get("Table Joins", False):
            complexity_scores['data_sources'] = 1  # Low
        elif datasource_count <= 3 and (features.get("Table Joins", False) or has_joins):
            complexity_scores['data_sources'] = 2  # Medium
        elif datasource_count > 3:
            complexity_scores['data_sources'] = 3  # High
        
        # 2. Number of Worksheets
        worksheet_count = count_worksheets_from_json({"dashboards": [dashboard_json]})
        if worksheet_count <= 3:
            complexity_scores['worksheets'] = 1  # Low
        elif 4 <= worksheet_count <= 7:
            complexity_scores['worksheets'] = 2  # Medium
        else:
            complexity_scores['worksheets'] = 3  # High
        
        # 3. Dashboard Features (based on tile count)
        tile_count = count_dashboard_tiles_from_json(dashboard_json)
        if tile_count <= 3:
            complexity_scores['dashboard_features'] = 1  # Low
        elif 4 <= tile_count <= 7:
            complexity_scores['dashboard_features'] = 2  # Medium
        else:
            complexity_scores['dashboard_features'] = 3  # High
        
        # 4. Filters, Groups, Sets
        basic_filters = features.get("Basic Filters", False)
        cascading_filters = features.get("Cascading Filter", False)
        group_feature = features.get("Group Feature", False)
        
        if basic_filters and group_feature and cascading_filters:
            complexity_scores['filters_groups'] = 3  # High
        elif basic_filters and group_feature and not cascading_filters:
            complexity_scores['filters_groups'] = 2  # Medium
        elif basic_filters and not group_feature and not cascading_filters:
            complexity_scores['filters_groups'] = 1  # Low
        
        # 5. Calculations
        basic_calc = features.get("Basic Calculated Field", False)
        table_calc = features.get("Table Calculations", False)
        cross_filtering = features.get("Cross-filtering", False)
        
        # Get LOD detection
        fixed_lod_present, complex_lod_present = type_of_lod_present_from_json(dashboard_json_with_context)
        
        if (basic_calc and table_calc and (complex_lod_present or cross_filtering) and not fixed_lod_present):
            complexity_scores['calculations'] = 3  # High
        elif basic_calc and table_calc and fixed_lod_present:
            complexity_scores['calculations'] = 2  # Medium
        elif basic_calc and not table_calc and not fixed_lod_present and not complex_lod_present:
            complexity_scores['calculations'] = 1  # Low
        
        # 6. Actions & Interactivity
        hover_interaction = features.get("Hover Over Interaction", False)
        highlight_action = features.get("Highlight Action", False)
        tooltip = features.get("Tooltip", False)
        labels = features.get("Labels", False)
        dynamic_switching_of_charts = features.get("Dynamic Switching of Charts", False)
        
        # High: Either hover OR highlight + tooltip + labels + dynamic switching
        if dynamic_switching_of_charts:
            complexity_scores['actions_interactivity'] = 5  # High
        elif (hover_interaction or highlight_action) and tooltip and labels and dynamic_switching_of_charts:
            complexity_scores['actions_interactivity'] = 3  # High
        # Medium: Either hover OR highlight + tooltip + labels (without dynamic switching)
        elif (hover_interaction or highlight_action) and tooltip and labels:
            complexity_scores['actions_interactivity'] = 2  # Medium
        # Low: Basic interactions
        elif highlight_action or tooltip or labels:
            complexity_scores['actions_interactivity'] = 1  # Low
        
        # 7. Parameters
        dynamic_calc_params = features.get("Dynamic Calculations using Parameters", False)
        single_value_param = features.get("Single-value parameter Top N", False)
        
        if dynamic_calc_params and single_value_param:
            complexity_scores['parameters'] = 3  # High
        elif dynamic_calc_params or single_value_param:
            complexity_scores['parameters'] = 2  # Medium
        elif not dynamic_calc_params and not single_value_param:
            complexity_scores['parameters'] = 1  # Low
        
        # 8. Charts (simplified assessment)
        chart_types = [
            "Bar Chart", "Line Chart", "Area Chart", "Pie Chart", "Text Table Chart"
        ]
        medium_charts = [
            "Shape chart", "Circle chart", "Square chart", "Map chart", "Dual Axis"
        ]
        high_charts = [
            "Gantt Bar", "Polygon", "Density"
        ]
        
        chart_count = sum(1 for chart in chart_types if features.get(chart, False))
        medium_chart_count = sum(1 for chart in medium_charts if features.get(chart, False))
        high_chart_count = sum(1 for chart in high_charts if features.get(chart, False))
        
        if chart_count > 0 and medium_chart_count == 0 and high_chart_count == 0:
            complexity_scores['charts'] = 1  # Low
        elif medium_chart_count > 0 and high_chart_count == 0:
            complexity_scores['charts'] = 2  # Medium
        elif high_chart_count > 0:
            complexity_scores['charts'] = 3  # High
        
        # Calculate overall complexity score for this dashboard
        total_score = sum(complexity_scores.values())
        
        if total_score <= 7:  # 1/3 of max (1-9) --- 9 max  - 33 % - 23 %
            return "Low"
        elif total_score <= 13:  # 2/3 of max (10-18)   - 18  - 33 %
            return "Medium"
        else:  # 3/3 of max (19-27) -- 27 - 33%
            return "High"
            
    except Exception as e:
        print(f"Error assessing dashboard complexity: {e}")
        return "Low"

def workbook_complexity_breakdown():
    """Main function to generate workbook complexity breakdown using JSON files"""
    load_dotenv()
    
    breakdown_data = []
    dashboard_complexity_counts = {"High": 0, "Medium": 0, "Low": 0}
    dashboard_names_by_complexity = {"High": [], "Medium": [], "Low": []}
    
    # Check if workbook directory exists
    if not WORKBOOK_DIR.exists():
        print(f"Warning: Workbook directory {WORKBOOK_DIR} does not exist.")
        return {"breakdown_table": [], "highlevel_table": []}
    
    # Process all workbook folders
    workbook_folders = [d for d in WORKBOOK_DIR.iterdir() if d.is_dir()]
    
    if not workbook_folders:
        print(f"No workbook folders found in {WORKBOOK_DIR}")
        return {"breakdown_table": [], "highlevel_table": []}
    
    print(f"Processing {len(workbook_folders)} workbook folder(s) from {WORKBOOK_DIR}")
    
    # Process each workbook folder (each represents a workbook)
    for workbook_folder in workbook_folders:
        # Look for processed_pipeline_output.json in each subfolder
        json_file = workbook_folder / "processed_pipeline_output.json"
        
        if not json_file.exists():
            print(f"Warning: processed_pipeline_output.json not found in {workbook_folder.name}")
            continue
        
        try:
            # Load JSON data
            json_data = load_json_file(json_file)
            if not json_data:
                continue
            
            workbook_name = workbook_folder.name
            
            # Get workbook-level counts
            total_dashboards = count_dashboards_from_json(json_data)
            
            # Process individual dashboards
            dashboards = get_dashboards_from_json(json_data)
            dashboard_complexities = []
            
            for dashboard_info in dashboards:
                dashboard_name = dashboard_info["name"]
                dashboard_json = dashboard_info["json_data"]
                
                # Create a JSON structure that includes this dashboard plus workbook-level data
                # This ensures we detect both workbook-level features (tables, connections, etc.)
                # and dashboard-level features (charts, filters, etc.)
                dashboard_json_with_context = {
                    "dashboards": [dashboard_json],
                    "tables": json_data.get("tables", []),
                    "relationships": json_data.get("relationships", []),
                    "connections": json_data.get("connections", []),
                    "calculated_fields": json_data.get("calculated_fields", []),
                    "parameters": json_data.get("parameters", []),
                    "actions": json_data.get("actions", [])
                }
                
                # Assess dashboard complexity
                try:
                    complexity = assess_dashboard_complexity_from_json(dashboard_json_with_context)
                    dashboard_complexities.append(complexity)
                    dashboard_names_by_complexity[complexity].append(f"{workbook_name}__{dashboard_name}")
                except Exception as e:
                    print(f"Error processing dashboard {dashboard_name} in workbook {workbook_name}: {e}")
                    dashboard_complexities.append("Low")
                    dashboard_names_by_complexity["Low"].append(f"{workbook_name}__{dashboard_name}")
            
            # Count complexity levels
            high_count = dashboard_complexities.count("High")
            medium_count = dashboard_complexities.count("Medium")
            low_count = dashboard_complexities.count("Low")
            
            # Count individual dashboards by complexity level
            dashboard_complexity_counts["High"] += high_count
            dashboard_complexity_counts["Medium"] += medium_count
            dashboard_complexity_counts["Low"] += low_count
            
            # Create dashboard summary
            dashboard_summary = f"Total dashboards - {total_dashboards}\n"
            dashboard_summary += f"High complex dashboards - {high_count}\n"
            dashboard_summary += f"Medium complex dashboards - {medium_count}\n"
            dashboard_summary += f"Low complex dashboards - {low_count}"
            
            # Add to breakdown data
            breakdown_data.append({
                "Workbook Name": workbook_name,
                "Dashboard Summary": dashboard_summary
            })
                
        except Exception as e:
            print(f"Error processing workbook {workbook_folder.name}: {e}")
            continue
    
    # Create high-level table data
    highlevel_data = [
        {
            "Complexity Level": "High", 
            "Dashboard Count": dashboard_complexity_counts["High"],
            "Dashboard Names": ", ".join(dashboard_names_by_complexity["High"])
        },
        {
            "Complexity Level": "Medium", 
            "Dashboard Count": dashboard_complexity_counts["Medium"],
            "Dashboard Names": ", ".join(dashboard_names_by_complexity["Medium"])
        },
        {
            "Complexity Level": "Low", 
            "Dashboard Count": dashboard_complexity_counts["Low"],
            "Dashboard Names": ", ".join(dashboard_names_by_complexity["Low"])
        }
    ]
    
    # Sort highlevel_data by Complexity level (Low → Medium → High)
    complexity_order = {"Low": 1, "Medium": 2, "High": 3}
    highlevel_data.sort(key=lambda x: complexity_order.get(x["Complexity Level"], 4))
    
    return {
        "breakdown_table": breakdown_data,
        "highlevel_table": highlevel_data
    }
