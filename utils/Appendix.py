import os
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import re


def get_dashboards_for_workbook(workbook_name, workbook_dir):
    dashboard_names = []
    # Try to find the workbook folder by name
    json_file = None
    for folder in workbook_dir.iterdir():
        if folder.is_dir() and (folder.name == workbook_name or workbook_name in folder.name):
            json_file = folder / "processed_pipeline_output.json"
            break
    
    if json_file and json_file.exists():
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
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
                dash_name = get_nested(dashboard, "name", default="")
                if dash_name:
                    dashboard_names.append(dash_name)
        except Exception as e:
            print(f"Error extracting dashboard names for {json_file}: {e}")
    return ", ".join(sorted(set(dashboard_names)))

# ---- Appendix Function ----
def appendix():
    load_dotenv()
    WORKBOOK_DIR = Path("output") / "workbook"

    output_records = []

    if not WORKBOOK_DIR.exists():
        print(f"Warning: Workbook directory {WORKBOOK_DIR} does not exist.")
        WORKBOOK_DIR.mkdir(parents=True, exist_ok=True)
        return pd.DataFrame(columns=["Datasets Used", "Owner ID", "Workbook Name", "Dashboard Name"])

    workbook_folders = [d for d in WORKBOOK_DIR.iterdir() if d.is_dir()]
    print(f"Found {len(workbook_folders)} workbook folder(s)")

    for workbook_folder in workbook_folders:
        # Look for processed_pipeline_output.json in each subfolder
        json_file = workbook_folder / "processed_pipeline_output.json"
        
        if not json_file.exists():
            print(f"Warning: processed_pipeline_output.json not found in {workbook_folder.name}")
            continue
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            workbook_name = workbook_folder.name
            
            def get_nested(data, *keys, default=None):
                for key in keys:
                    if isinstance(data, dict):
                        data = data.get(key, default)
                    else:
                        return default
                    if data is None:
                        return default
                return data
            
            # Extract data sources from connections
            datasource_map = []
            connections = get_nested(json_data, "connections", default=[])
            if not isinstance(connections, list):
                connections = [connections] if connections else []
            
            for conn in connections:
                conn_type = get_nested(conn, "type", default="Unknown")
                conn_name = get_nested(conn, "name", default="Unnamed Dataset")
                if not conn_name or conn_name == "Unnamed Dataset":
                    # Try to get dataset name if available
                    dataset = get_nested(conn, "dataset", default="")
                    if dataset:
                        conn_name = dataset
                datasource_map.append(f"{conn_name} ({conn_type})")
            
            all_sources_combined = ", ".join(sorted(set(datasource_map))) if datasource_map else "Unknown"
            
            # Extract dashboard names
            dashboard_names = get_dashboards_for_workbook(workbook_name, WORKBOOK_DIR)
            
            # Extract owner ID if available
            owner_id = get_nested(json_data, "owner_id", default="Squareshift")
            
            output_records.append({
                "Workbook Name": workbook_name,
                "Dashboard Name": dashboard_names,
                "Owner ID": owner_id,
                "Datasets Used": all_sources_combined
            })
        except Exception as e:
            print(f"Error processing {workbook_folder.name}: {e}")
    
    output_records = pd.DataFrame(output_records)
    output_records = output_records.drop_duplicates()
    column_order = ["Datasets Used", "Owner ID", "Workbook Name", "Dashboard Name"]
    output_records = output_records[column_order]
    
    return output_records
