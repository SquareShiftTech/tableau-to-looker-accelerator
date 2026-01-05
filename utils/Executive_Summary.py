import os
import re
import json
from pathlib import Path
from collections import defaultdict, Counter
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from fuzzywuzzy import process
from utils.Complexity_Analysis import detect_features_from_json, load_json_file
# Note: assess_dashboard_complexity_from_json is imported inside the function to avoid circular import

def get_dashboards_from_json(json_data):
    """Extract dashboard information from JSON data."""
    dashboards = []
    
    def get_nested(data, *keys, default=None):
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, default)
            else:
                return default
            if data is None:
                return default
        return data
    
    dashboard_list = get_nested(json_data, "dashboards", default=[])
    
    for dashboard in dashboard_list:
        dashboard_name = get_nested(dashboard, "name", default="")
        if not dashboard_name:
            continue
        
        worksheets = get_nested(dashboard, "worksheet", default=[])
        
        worksheet_names = []
        for worksheet in worksheets:
            ws_name = get_nested(worksheet, "name", default="")
            if ws_name:
                worksheet_names.append(ws_name)
        
        dashboards.append({
            "name": dashboard_name,
            "worksheets": worksheet_names,
            "json_data": dashboard
        })
    
    return dashboards

def executive_summary():
    load_dotenv()
    PROJECT_ID = os.getenv("PROJECT_ID")
    DATASET_TABLE = os.getenv("DATASET_TABLE")
    WORKBOOK_DIR = Path("output") / "workbook"

    def normalize_key(s):
        return re.sub(r'[^a-zA-Z0-9]', '', s).lower()

    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT Area, Features, Complexity FROM `{PROJECT_ID}.{DATASET_TABLE}`"
        bq_df = bq_client.query(query).to_dataframe()
        bq_df = bq_df.astype({"Area": "string", "Features": "string", "Complexity": "string"})
       
    except Exception as e:
        print(f"Error connecting to BigQuery: {e}")
        bq_df = pd.DataFrame(columns=["Area", "Features", "Complexity"])

    feature_to_area = {normalize_key(row["Features"]): row["Area"] for _, row in bq_df.iterrows()}
    feature_to_complexity = {normalize_key(row["Features"]): row["Complexity"] for _, row in bq_df.iterrows()}
    priority_order = {"High": 3, "Medium": 2, "Low": 1, "Opportunity": 0, "Unknown": -1}

    feature_hits = defaultdict(set)         # Feature -> set of workbooks
    dashboard_feature_hits = defaultdict(set) # Feature -> set of dashboards (for percent-in-dashboards)
    total_dashboards = 0
    all_dashboard_ids = set()  # Unique dashboards for percent calculation

    if not WORKBOOK_DIR.exists():
        print(f"Warning: Workbook directory {WORKBOOK_DIR} does not exist. Creating it.")
        WORKBOOK_DIR.mkdir(parents=True, exist_ok=True)
        return {
            "date_generated": datetime.now().strftime("%B %d, %Y - %I:%M %p"),
            "environment": "JSON Files",
            "executive_summary": "No workbook folders found.",
            "total_workbooks": 0,
            "total_dashboards": 0,
            "overall_complexity_score": "Unknown",
            "estimated_migration_effort": "Yet to be done"
        }, pd.DataFrame(columns=["Area", "Complexity / Impact", "Workbook", "Dashboard"])

    # Process all workbook folders
    workbook_folders = [d for d in WORKBOOK_DIR.iterdir() if d.is_dir()]
    total_workbooks = len(workbook_folders)
    print(f"Processing {total_workbooks} workbook folder(s) from {WORKBOOK_DIR}")

    for workbook_folder in workbook_folders:
        # Look for processed_pipeline_output.json in each subfolder
        json_file = workbook_folder / "processed_pipeline_output.json"
        
        if not json_file.exists():
            print(f"Warning: processed_pipeline_output.json not found in {workbook_folder.name}")
            continue
        
        try:
            json_data = load_json_file(json_file)
            if not json_data:
                continue
            
            workbook_name = workbook_folder.name
            
            # Extract dashboards from JSON
            dashboards = get_dashboards_from_json(json_data)
            total_dashboards += len(dashboards)
            
            # Process each dashboard
            for dashboard_info in dashboards:
                dashboard_name = dashboard_info["name"]
                dashboard_json = dashboard_info["json_data"]
                dashboard_id = f"{workbook_name}__{dashboard_name}"
                all_dashboard_ids.add(dashboard_id)
                
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
                
                # Analyze dashboard for features (includes both workbook and dashboard level)
                features = detect_features_from_json(dashboard_json_with_context)
                
                for f, used in features.items():
                    if used:
                        feature_hits[f].add(workbook_name)
                        dashboard_feature_hits[f].add(dashboard_id)
        except Exception as ex:
            print(f"Error processing {workbook_folder.name}: {ex}")

    # Build area-level statistics
    # Track: area -> {workbooks: set, dashboards: set, features: set, complexities: list}
    area_stats = defaultdict(lambda: {"workbooks": set(), "dashboards": set(), "features": set(), "complexities": []})
    
    # Helper function to get complexity with fuzzy matching fallback
    def get_complexity_with_fuzzy(feature_name, feature_key):
        """Get complexity for a feature, using fuzzy matching directly from BigQuery."""
        # First try exact match with normalized key
        complexity = feature_to_complexity.get(feature_key, "Unknown")      
        
        return complexity if complexity and not pd.isna(complexity) else "Unknown"
    
    # Helper function to get area with fuzzy matching fallback
    def get_area_with_fuzzy(feature_name, feature_key):
        """Get area for a feature, using fuzzy matching directly from BigQuery."""
        area = feature_to_area.get(feature_key)
        
        return area if area and not pd.isna(area) else ""
    
    # Process feature hits to build area-level stats
    for feature, wbs in feature_hits.items():
        key = normalize_key(feature)
        area = get_area_with_fuzzy(feature, key)
        if pd.notna(area) and area != "":
            area_stats[area]["workbooks"].update(wbs)
            area_stats[area]["features"].add(feature)
            # Add complexity for this feature
            complexity = get_complexity_with_fuzzy(feature, key)
            area_stats[area]["complexities"].append(complexity)
            # Debug: Log if complexity is Unknown
            if complexity == "Unknown":
                print(f"Warning: Feature '{feature}' in area '{area}' has Unknown complexity")
                if not bq_df.empty:
                    # Show what's available in BigQuery for this area
                    area_features = bq_df[bq_df["Area"] == area]["Features"].tolist()
                    print(f"  Available features in '{area}' area: {area_features[:5]}")  # Show first 5
        else:
            # Debug: Log when area lookup fails
            print(f"Warning: Could not find area for feature '{feature}' (normalized: '{key}')")
            if not bq_df.empty:
                # Try to find similar features in BigQuery
                similar_features = bq_df[bq_df["Features"].str.contains(feature.split()[0] if feature.split() else "", case=False, na=False)]["Features"].tolist()
                if similar_features:
                    print(f"  Similar features in BigQuery: {similar_features[:5]}")
    
    # Process dashboard feature hits to build area-level dashboard stats
    for feature, dashboard_ids in dashboard_feature_hits.items():
        key = normalize_key(feature)
        area = get_area_with_fuzzy(feature, key)
        if pd.notna(area) and area != "":
            area_stats[area]["dashboards"].update(dashboard_ids)

    # Get all unique areas from BigQuery to ensure we show all areas
    all_areas_from_bq = set()
    if not bq_df.empty:
        all_areas_from_bq = set(bq_df["Area"].dropna().unique())
    
    # Build summary for all areas (from BigQuery)
    summary = []
    dashboard_count = len(all_dashboard_ids) if all_dashboard_ids else 1
    
    # Process areas that have detected features
    for area, info in area_stats.items():
        # Calculate workbook percentage
        workbook_percent = (len(info["workbooks"]) / total_workbooks * 100) if total_workbooks > 0 else 0
        workbook_percent = min(workbook_percent, 100)
        workbook_percent = round(workbook_percent, 1)
        
        # Calculate dashboard percentage (dashboards that use ANY feature from this area)
        dashboard_percent = (len(info["dashboards"]) / dashboard_count * 100) if dashboard_count > 0 else 0
        dashboard_percent = min(dashboard_percent, 100)
        dashboard_percent = round(dashboard_percent, 1)
        
        # Only include areas that have at least some usage (> 0%)
        if workbook_percent == 0 and dashboard_percent == 0:
            continue
        
        # Determine complexity (most common complexity in this area)
        # Filter out "Unknown" complexities first - only use them if ALL are Unknown
        known_complexities = [c for c in info["complexities"] if c != "Unknown"]
        if known_complexities:
            complexity_counts = Counter(known_complexities)
        
        sorted_complexities = sorted(
            complexity_counts.items(),
            key=lambda x: (-x[1], -priority_order.get(x[0], -1))
        )
        final_complexity = sorted_complexities[0][0] if sorted_complexities else "Unknown"
        
        summary.append({
            "Area": area,
            "Workbook": f"Used in {workbook_percent}% of workbooks",
            "Dashboard": f"Used in {dashboard_percent}% of dashboards",
            "Complexity / Impact": final_complexity
        })
    
    # Only include areas that have at least some detected features (workbook or dashboard usage > 0%)
    # Do NOT add areas from BigQuery that have 0% usage - they're not relevant

    if summary:
        summary_df = pd.DataFrame(summary).sort_values(by="Area").reset_index(drop=True)
        
        # Calculate overall complexity and dashboard complexity distribution
        # Use the Complexity / Impact column directly (no need to extract with hardcoded regex)
        complexity_counts = Counter(summary_df["Complexity / Impact"])
        sorted_complexities = sorted(
            complexity_counts.items(),
            key=lambda x: (-x[1], -priority_order.get(x[0], -1))
        )
        overall_complexity = sorted_complexities[0][0] if sorted_complexities else "Unknown"
        
        # Calculate dashboard count by complexity level using the same method as workbook_complexity_breakdown
        # Re-process workbooks to assess dashboard complexity using assess_dashboard_complexity_from_json
        # Import here to avoid circular import (lazy import)
        from utils.workbook_complexity_breakdown import assess_dashboard_complexity_from_json
        
        dashboard_complexity_counts = {"High": 0, "Medium": 0, "Low": 0}
        
        # Re-process workbooks to get dashboard complexity (same logic as workbook_complexity_breakdown)
        for workbook_folder in workbook_folders:
            json_file = workbook_folder / "processed_pipeline_output.json"
            if not json_file.exists():
                continue
            
            try:
                json_data = load_json_file(json_file)
                if not json_data:
                    continue
                
                # Get dashboards from JSON
                dashboards = get_dashboards_from_json(json_data)
                
                for dashboard_info in dashboards:
                    dashboard_json = dashboard_info["json_data"]
                    
                    # Create a JSON structure that includes this dashboard plus workbook-level data
                    dashboard_json_with_context = {
                        "dashboards": [dashboard_json],
                        "tables": json_data.get("tables", []),
                        "relationships": json_data.get("relationships", []),
                        "connections": json_data.get("connections", []),
                        "calculated_fields": json_data.get("calculated_fields", []),
                        "parameters": json_data.get("parameters", []),
                        "actions": json_data.get("actions", [])
                    }
                    
                    # Assess dashboard complexity using the same method as workbook_complexity_breakdown
                    try:
                        complexity = assess_dashboard_complexity_from_json(dashboard_json_with_context)
                        dashboard_complexity_counts[complexity] += 1
                    except Exception as e:
                        print(f"Error assessing dashboard complexity: {e}")
                        dashboard_complexity_counts["Low"] += 1
            except Exception as e:
                print(f"Error processing {workbook_folder.name} for complexity: {e}")
                continue
        
        # Format as highlevel_table (without dashboard names)
        highlevel_table = [
            {
                "Complexity Level": "Low",
                "Dashboard Count": dashboard_complexity_counts["Low"]
            },
            {
                "Complexity Level": "Medium",
                "Dashboard Count": dashboard_complexity_counts["Medium"]
            },
            {
                "Complexity Level": "High",
                "Dashboard Count": dashboard_complexity_counts["High"]
            }
        ]
        
        # Sort by Complexity level (Low → Medium → High)
        complexity_order = {"Low": 1, "Medium": 2, "High": 3}
        highlevel_table.sort(key=lambda x: complexity_order.get(x["Complexity Level"], 4))
    else:
        summary_df = pd.DataFrame(columns=["Area", "Complexity / Impact", "Workbook", "Dashboard"])
        overall_complexity = "Unknown"
        highlevel_table = []


    date_generated = datetime.now().strftime("%B %d, %Y - %I:%M %p")
    env_name = "JSON Files"
    exec_summary = (
        f"This assessment analyzed the Tableau environment "
        f"({total_workbooks} workbooks, {total_dashboards} dashboards) focusing on inventory, "
        f"usage, and complexity to inform the Looker migration strategy."
    )

    metadata = {
        "date_generated": date_generated,
        "environment": env_name,
        "executive_summary": exec_summary,
        "total_workbooks": total_workbooks,
        "total_dashboards": total_dashboards,
        "overall_complexity_score": overall_complexity,
        "estimated_migration_effort": "Yet to be done",
        "highlevel_table": highlevel_table
    }
    column_order = ["Area", "Complexity / Impact", "Workbook", "Dashboard"]
    summary_df = summary_df[column_order]
    return metadata, summary_df
