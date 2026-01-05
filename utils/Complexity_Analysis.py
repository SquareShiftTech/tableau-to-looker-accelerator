import os
import re
import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
from google.cloud import bigquery
from fuzzywuzzy import process

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_TABLE = os.getenv("DATASET_TABLE")
WORKBOOK_DIR = Path("output") / "workbook"  # Directory containing workbook folders

def normalize_key(s):
    """Normalize feature names by removing non-alphanumeric characters and converting to lowercase."""
    if pd.isna(s) or s is None:
        return ""
    return re.sub(r'[^a-zA-Z0-9]', '', str(s).lower().strip())

def load_json_file(file_path):
    """Load JSON file and return parsed data."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None

def detect_features_from_json(json_data):
    """Detect features from JSON data based on the provided mapping rules."""
    features = {
        "Bar Chart": False, "Line Chart": False, "Area Chart": False, "Pie Chart": False, "Bubble Chart": False, "Map chart": False, "Text Table Chart": False, "Shape chart":False, "Circle chart":False, "Square chart":False, "Gantt Bar": False, "Polygon": False, "Density": False,
        "Percentage Calculation": False, "Static Filter": False, "Single-value parameter Top N": False, "Union": False,
        "Single Table Source": False, "Table Joins": False, "Tooltip": False, "Data Warehouse Connection": False,
        "Single Tile dashboard": False, "Multi Tile dashboard": False, "Basic Calculated Field": False,
        "Custom SQL": False, "Multiple Worksheets": False, "Table Calculations": False,
        "Basic Filters": False, "Hover Over Interaction": False, "Group Feature": False,
        "Cascading Filter": False, "Highlight Action": False, "Dynamic Calculations using Parameters": False,
        "Drill Down to Dashboard": False, "Hierarchy Feature": False, "Labels": False,
        # "LOD expressions": False, 
        "Simple LOD": False, "Advanced LOD": False, "Date Calculation": False,
        "Cross-filtering": False,"Dynamic Switching of Charts": False, "Dual Axis": False
    }

    if not json_data:
        return features

    try:
        # Helper function to safely get nested values
        def get_nested(data, *keys, default=None):
            for key in keys:
                if isinstance(data, dict):
                    data = data.get(key, default)
                elif isinstance(data, list):
                    if isinstance(key, int) and 0 <= key < len(data):
                        data = data[key]
                    else:
                        return default
                else:
                    return default
                if data is None:
                    return default
            return data

        # Chart Type - Under: dashboards → worksheet → visualization → chart_type
        dashboards = get_nested(json_data, "dashboards", default=[])
       
        for dashboard in dashboards:
            worksheets = get_nested(dashboard, "worksheet", default=[])
           
            for worksheet in worksheets:
                visualization = get_nested(worksheet, "visualization", default={})
                chart_type = get_nested(visualization, "chart_type", default="").lower()
                
                if "bar" in chart_type:
                    features["Bar Chart"] = True
                if "line" in chart_type:
                    features["Line Chart"] = True
                if "area" in chart_type:
                    features["Area Chart"] = True
                if "pie" in chart_type:
                    features["Pie Chart"] = True
                if "bubble" in chart_type or "circle" in chart_type:
                    features["Bubble Chart"] = True
                if "map" in chart_type:
                    features["Map chart"] = True
                if "text table" in chart_type or "texttable" in chart_type.replace(" ", ""):
                    features["Text Table Chart"] = True
                if "shape" in chart_type:
                    features["Shape chart"] = True
                if "circle" in chart_type:
                    features["Circle chart"] = True
                if "square" in chart_type:
                    features["Square chart"] = True
                if "gantt" in chart_type:
                    features["Gantt Bar"] = True
                if "polygon" in chart_type:
                    features["Polygon"] = True
                if "density" in chart_type:
                    features["Density"] = True
                
                # Labels Used - Under visualization, check "show_labels"
                show_labels = get_nested(visualization, "show_labels", default=False)
                if show_labels:
                    features["Labels"] = True

        # Single Table Source - Under tables, check if only one table exists
        tables = get_nested(json_data, "tables", default=[])
        
        if len(tables) == 1:
                features["Single Table Source"] = True

        # Table Joins - Under relationships, check "join-type"
        relationships = get_nested(json_data, "relationships", default=[])
        
        for rel in relationships:
            join_type = get_nested(rel, "join-type", default="")
            if join_type:
                features["Table Joins"] = True
                break

        # Union - Under relationships, check "relationship_type": "union"
        for rel in relationships:
            relationship_type = get_nested(rel, "relationship_type", default="").lower()
            if relationship_type == "union":
                features["Union"] = True
                break

        # Data Warehouse Connection - Under connections, fetch the "type"
        connections = get_nested(json_data, "connections", default=[])
        
        for conn in connections:
            conn_type = get_nested(conn, "type", default="")
            if conn_type:
                features["Data Warehouse Connection"] = True
                break
        
        # Custom SQL - Under tables, check where "relation_type": "Custom_Sql"
        for table in tables:
            relation_type = get_nested(table, "relation_type", default="")
            if relation_type == "Custom_Sql":
                features["Custom SQL"] = True
                break

        # Calculated Field - Under calculated_fields → calculation verify: original formula exists AND does not contain LOD keywords
        calculated_fields = get_nested(json_data, "calculated_fields", default=[])
       
        for calc_field in calculated_fields:
            calculation = get_nested(calc_field, "calculation", default={})
            formula = get_nested(calculation, "original_formula", default="")
            if formula:
                formula_upper = formula.upper()
                if not any(kw in formula_upper for kw in ["INCLUDE", "EXCLUDE", "FIXED"]):
                        features["Basic Calculated Field"] = True
                        break
        
        # LOD Expression - Formula starts with {FIXED, {INCLUDE, or {EXCLUDE
        # Distinguish between Simple LOD (FIXED) and Advanced LOD (INCLUDE/EXCLUDE)
        for calc_field in calculated_fields:
            calculation = get_nested(calc_field, "calculation", default={})
            formula = get_nested(calculation, "original_formula", default="")
            if formula:
                formula_cleaned = formula.replace(" ", "").upper()
                if formula_cleaned.startswith("{FIXED"):
                    features["Simple LOD"] = True
                    # features["LOD expressions"] = True  # Keep for backward compatibility
                elif any(formula_cleaned.startswith(f"{{{kw}") for kw in ["INCLUDE", "EXCLUDE"]):
                    features["Advanced LOD"] = True
                    # features["LOD expressions"] = True  # Keep for backward compatibility
                # if features["LOD expressions"]:
                #     break

        # Date Calculation - In worksheet fields, check datatype = datetime or date. If formula references such fields
        date_fields = set()
        for dashboard in dashboards:
            worksheets_in_dash = get_nested(dashboard, "worksheet", default=[])
           
            for worksheet in worksheets_in_dash:
                fields = get_nested(worksheet, "fields", default=[])
                
                for field in fields:
                    datatype = get_nested(field, "datatype", default="").lower()
                    if datatype in ["datetime", "date"]:
                        field_name = get_nested(field, "name", default="")
                        if field_name:
                            date_fields.add(field_name.strip())
        
        # Check if any calculated field references date fields
        for calc_field in calculated_fields:
            calculation = get_nested(calc_field, "calculation", default={})
            formula = get_nested(calculation, "original_formula", default="")
            if formula:
                for date_field in date_fields:
                    if date_field.replace(" ", "") in formula.replace(" ", ""):
                        features["Date Calculation"] = True
                        break
                if features["Date Calculation"]:
                    break

        # Dynamic Parameter Calculation - Under dashboards → worksheet → parameter, check "parameter-type": "Dynamic-parameter" or "param_domain_type": "Dynamic-parameter"
        for dashboard in dashboards:
            worksheets = get_nested(dashboard, "worksheet", default=[])
            if not isinstance(worksheets, list):
                worksheets = [worksheets] if worksheets else []
            
            for worksheet in worksheets:
                parameters = get_nested(worksheet, "parameter", default=[])
              
                for param in parameters:
                    param_type = get_nested(param, "parameter-type", default="")
                    domain_type = get_nested(param, "param_domain_type", default="")
                    if param_type == "Dynamic-parameter" or domain_type == "Dynamic-parameter":
                        features["Dynamic Calculations using Parameters"] = True
                        break
                if features["Dynamic Calculations using Parameters"]:
                    break
            if features["Dynamic Calculations using Parameters"]:
                        break

        # Single-Value Parameter (Top N) - Under parameters, check "parameter-type": "single-value-parameter"
        parameters = get_nested(json_data, "parameters", default=[])
        
        for param in parameters:
            param_type = get_nested(param, "parameter-type", default="")
            if param_type == "single-value-parameter":
                features["Single-value parameter Top N"] = True
                break

        # Cascading Filter - Under dashboards → worksheet → cascading_filter, detect presence of "has_cascading_filter"
        for dashboard in dashboards:
            worksheets = get_nested(dashboard, "worksheet", default=[])
            if not isinstance(worksheets, list):
                worksheets = [worksheets] if worksheets else []
            
            for worksheet in worksheets:
                cascading_filter = get_nested(worksheet, "cascading_filter", default={})
                has_cascading = get_nested(cascading_filter, "has_cascading_filter", default=False)
                if has_cascading:
                    features["Cascading Filter"] = True
                    break
            if features["Cascading Filter"]:
                break

        # Hover Interaction - Under actions, check "activation": "on-hover"
        actions = get_nested(json_data, "actions", default=[])
            
        for action in actions:
            activation = get_nested(action, "activation", default="")
            if activation == "on-hover":
                features["Hover Over Interaction"] = True
                break

        # Highlight Action - Under actions → command, detect "tsc:brush" or "tsc:tsl-filter"
        for action in actions:
            command = get_nested(action, "command", default="")
            if command in ["tsc:brush", "tsc:tsl-filter"]:
                features["Highlight Action"] = True
                break

        # Drill Down to Dashboard - Under actions, detect "activation": "on-select" AND "command": "tsc:filter" or "command": "tsc:tsl-filter"
        for action in actions:
            activation = get_nested(action, "activation", default="")
            command = get_nested(action, "command", default="")
            if activation == "on-select" and command in ["tsc:filter", "tsc:tsl-filter"]:
                features["Drill Down to Dashboard"] = True
                break

        # Hierarchy Chart - Under dashboards → worksheet → hierarchy_usage, check "has_hierarchy_usage"
        for dashboard in dashboards:
            worksheets = get_nested(dashboard, "worksheet", default=[])
            if not isinstance(worksheets, list):
                worksheets = [worksheets] if worksheets else []
            
            for worksheet in worksheets:
                hierarchy_usage = get_nested(worksheet, "hierarchy_usage", default={})
                has_hierarchy = get_nested(hierarchy_usage, "has_hierarchy_usage", default=False)
                if has_hierarchy:
                    features["Hierarchy Feature"] = True
                    break
            if features["Hierarchy Feature"]:
                break

        # Group Feature - Under calculated fields, check "calculation_class": "categorical-bin"
        for calc_field in calculated_fields:
            calc_class = get_nested(calc_field, "calculation_class", default="")
            if calc_class == "categorical-bin":
                features["Group Feature"] = True
                break

        # Single Tile Dashboard - Under dashboard check if there is only one worksheet
        # Multi Tile Dashboard - Under dashboard check if there are multiple worksheets
        for dashboard in dashboards:
            worksheets_in_dash = get_nested(dashboard, "worksheet", default=[])
            if not isinstance(worksheets_in_dash, list):
                worksheets_in_dash = [worksheets_in_dash] if worksheets_in_dash else []
            
            if len(worksheets_in_dash) == 1:
                features["Single Tile dashboard"] = True
            elif len(worksheets_in_dash) > 1:
                features["Multi Tile dashboard"] = True

        # Multiple Worksheets - Count worksheets across all dashboards. More than one → multiple
        total_worksheets = 0
        for dashboard in dashboards:
            worksheets = get_nested(dashboard, "worksheet", default=[])
            total_worksheets += len(worksheets)
        
        if total_worksheets > 1:
            features["Multiple Worksheets"] = True

        # Static Filter - Under groupfilter_logic in worksheets, check "function": "filter"
        for dashboard in dashboards:
            worksheets = get_nested(dashboard, "worksheet", default=[])
            
            
            for worksheet in worksheets:
                groupfilter_logic = get_nested(worksheet, "groupfilter_logic", default=[])
            
                
                for gf in groupfilter_logic:
                    func = get_nested(gf, "function", default="")
                    if func == "filter":
                        features["Static Filter"] = True
                        break
                if features["Static Filter"]:
                    break
            if features["Static Filter"]:
                break

        # Filters - Under dashboards → worksheet, lookup "filters"
        for dashboard in dashboards:
            worksheets = get_nested(dashboard, "worksheet", default=[])
          
            
            for worksheet in worksheets:
                filters = get_nested(worksheet, "filters", default=[])
                if filters:
                    features["Basic Filters"] = True
                    break
            if features["Basic Filters"]:
                break

        # Note: Tooltip, Percentage Calculation, Table Calculations are not currently available in JSON
        # These remain False as per the mapping rules

    except Exception as e:
        print(f"Error parsing JSON: {e}")

    return features

def complexity_analysis():
    bq_df = None
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT Area, Features, Complexity FROM `{PROJECT_ID}.{DATASET_TABLE}`"
        bq_df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
        bq_df = bq_df.astype({"Area": "string", "Features": "string", "Complexity": "string"})
        bq_df["Features"] = bq_df["Features"].str.strip()
        
        # Create normalized maps for exact matching
        normalized_complexity_map = {
            normalize_key(k): v for k, v in zip(bq_df["Features"], bq_df["Complexity"])
        }
        normalized_area_map = {
            normalize_key(k): v for k, v in zip(bq_df["Features"], bq_df["Area"])
        }
        
        # Create a list of normalized feature names for fuzzy matching
        bq_normalized_features = [normalize_key(f) for f in bq_df["Features"].tolist()]
        
        print(f"Loaded {len(bq_df)} features from BigQuery")
        print(f"Sample BigQuery features: {bq_df['Features'].head(10).tolist()}")
    except Exception as e:
        print(f"Error connecting to BigQuery: {e}")
        normalized_complexity_map = {}
        normalized_area_map = {}
        bq_normalized_features = []
        bq_df = pd.DataFrame()

    feature_hits = defaultdict(set)
    dashboard_feature_hits = defaultdict(set)

    # Process workbook folders from WORKBOOK_DIR
    if not WORKBOOK_DIR.exists():
        print(f"Warning: Workbook directory {WORKBOOK_DIR} does not exist. Creating it.")
        WORKBOOK_DIR.mkdir(parents=True, exist_ok=True)
        return pd.DataFrame(columns=["Feature Area", "Feature", "Complexity", "Workbooks Affected", "Dashboards Affected"])

    # Process all JSON files
    workbook_folders = [d for d in WORKBOOK_DIR.iterdir() if d.is_dir()]
    print(f"Processing {len(workbook_folders)} workbook folder(s) from {WORKBOOK_DIR}")

    for workbook_folder in workbook_folders:
        # Look for processed_pipeline_output.json in each subfolder
        json_file = workbook_folder / "processed_pipeline_output.json"
        
        if not json_file.exists():
            print(f"Warning: processed_pipeline_output.json not found in {workbook_folder.name}")
            continue
        try:
            json_data = load_json_file(json_file)
            if json_data:
                features = detect_features_from_json(json_data)
                file_name = workbook_folder.name  # Use folder name as workbook identifier
                
                for feature, found in features.items():
                    if found:
                        feature_hits[feature].add(file_name)
                        dashboard_feature_hits[feature].add(file_name)
        except Exception as e:
            print(f"Error processing {workbook_folder.name}: {e}")

    # Build summary - only include features that were actually found (have at least 1 workbook or dashboard)
    # Only process features that were detected in the workbooks
    all_detected_features = set(feature_hits.keys()) | set(dashboard_feature_hits.keys())
    
    if not all_detected_features:
        print("Warning: No features detected in any workbooks.")
        return {}
    
    summary_rows = []
    for feature in all_detected_features:
        workbooks = feature_hits.get(feature, set())
        dashboards = dashboard_feature_hits.get(feature, set())
        wb_count = len(workbooks)
        dash_count = len(dashboards)
        
        # Only include features that have at least 1 workbook or dashboard affected
        if wb_count == 0 and dash_count == 0:
            continue
        
        # Get complexity and area from BigQuery using fuzzy matching directly
        feature_key = normalize_key(feature)
        
        complexity = normalized_complexity_map.get(feature_key, "Unknown")
        feature_area = normalized_area_map.get(feature_key, "Unknown")
        
        
        summary_rows.append({
            "Feature Area": feature_area,
            "Feature": feature,
            "Complexity": complexity,
            "Workbooks Affected": wb_count,
            "Dashboards Affected": dash_count
        })

    if not summary_rows:
        print("Warning: No features with workbooks or dashboards affected found.")
        return {}
    
    # Group by Feature Area
    grouped_by_area = defaultdict(list)
    for row in summary_rows:
        feature_area = row["Feature Area"]
        # Create key like "Charts Complexity Breakdown" from "Charts"
        area_key = f"{feature_area} Complexity Breakdown"
        grouped_by_area[area_key].append(row)
    
    # Sort each group by Workbooks Affected (descending)
    result = {}
    for area_key, features in grouped_by_area.items():
        # Sort by Workbooks Affected descending, then by Dashboards Affected descending
        sorted_features = sorted(
            features,
            key=lambda x: (-x["Workbooks Affected"], -x["Dashboards Affected"])
        )
        result[area_key] = sorted_features
    
    return result
