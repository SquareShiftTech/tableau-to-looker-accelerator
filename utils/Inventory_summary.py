import os
import json
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv


def inventory_summary():
    # Load environment variables from .env
    load_dotenv()

    WORKBOOK_DIR = Path("output") / "workbook"

    def count_dashboards_and_worksheets(json_data):
        dashboards, worksheets = 0, 0
        try:
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
            
            dashboards = len(dashboard_list)
            
            # Worksheets are nested inside dashboards, so count all worksheets across all dashboards
            worksheets = 0
            for dashboard in dashboard_list:
                worksheet_list = get_nested(dashboard, "worksheet", default=[])
                worksheets += len(worksheet_list)
        except:
            pass
        return dashboards, worksheets

    if not WORKBOOK_DIR.exists():
        print(f"Warning: Workbook directory {WORKBOOK_DIR} does not exist.")
        WORKBOOK_DIR.mkdir(parents=True, exist_ok=True)
        return pd.DataFrame(columns=["Asset Type", "Count"])

    # Process workbook folders
    workbook_folders = [d for d in WORKBOOK_DIR.iterdir() if d.is_dir()]
    total_workbooks = len(workbook_folders)

    total_dashboards_xml = 0
    total_worksheets_xml = 0
    unique_table_names = set()  # Track unique table names for Total Views
    published_datasource_count = 0
    embedded_datasource_count = 0

    def get_nested(data, *keys, default=None):
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, default)
            else:
                return default
            if data is None:
                return default
        return data

    for workbook_folder in workbook_folders:
        # Look for processed_pipeline_output.json in each subfolder
        json_file = workbook_folder / "processed_pipeline_output.json"
        
        if not json_file.exists():
            print(f"Warning: processed_pipeline_output.json not found in {workbook_folder.name}")
            continue
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            dashboards, worksheets = count_dashboards_and_worksheets(json_data)
            total_dashboards_xml += dashboards
            total_worksheets_xml += worksheets
            
            # Count views from tables - extract unique table names
            # Total Views = count of unique table names across all workbooks
            tables = get_nested(json_data, "tables", default=[])
           
            for table in tables:
                table_name = get_nested(table, "name", default=None)
                if table_name and table_name.strip():  # Only add non-empty names
                    unique_table_names.add(table_name.strip())  # Track unique names (case-sensitive)
            
            # Count published vs embedded data sources
            connections = get_nested(json_data, "connections", default=[])
           
            for conn in connections:
                datasource_id = get_nested(conn, "datasource_id", default=None)
                if datasource_id:
                    published_datasource_count += 1
                else:
                    embedded_datasource_count += 1
        except Exception as e:
            print(f"Error processing {workbook_folder.name}: {e}")

    # Total Views = count of unique table names
    total_views = len(unique_table_names)
    
    summary_data = [
        {"Asset Type": "Total Workbooks Assessed", "Count": total_workbooks},
        {"Asset Type": "Total Views", "Count": total_views},  # Counted from tables array: unique table names count
        {"Asset Type": "Total Dashboards", "Count": total_dashboards_xml},
        {"Asset Type": "Total Worksheets", "Count": total_worksheets_xml},
        {"Asset Type": "Total Published Data Sources", "Count": published_datasource_count},  # Counted from connections with datasource_id
        {"Asset Type": "Total Embedded Data Sources", "Count": embedded_datasource_count},  # Counted from connections without datasource_id
        # {"Asset Type": "Active Users (Last 90 days)", "Count": 0},  # Commented out: Server-level metadata, not available in JSON files
        # {"Asset Type": "Total Licensed Users", "Count": 0},  # Commented out: Server-level metadata, not available in JSON files
    ]

    return pd.DataFrame(summary_data)
