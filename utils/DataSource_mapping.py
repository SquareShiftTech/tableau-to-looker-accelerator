import os
import json
from dotenv import load_dotenv
import pandas as pd
from collections import defaultdict
from pathlib import Path

def datasource_mapping():
    
    # Load .env
    load_dotenv()

    WORKBOOK_DIR = Path("output") / "workbook"

    source_counter = defaultdict(lambda: {"count": 0, "db_type": "Unknown", "type": "Unknown"})

    if not WORKBOOK_DIR.exists():
        print(f"Warning: Workbook directory {WORKBOOK_DIR} does not exist.")
        WORKBOOK_DIR.mkdir(parents=True, exist_ok=True)
        return pd.DataFrame(columns=["Tableau Source", "Type", "DB Type", "Usage"])

    # Helper function to safely get nested values
    def get_nested(data, *keys, default=None):
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, default)
            else:
                return default
            if data is None:
                return default
        return data

    # Process all workbook folders
    workbook_folders = [d for d in WORKBOOK_DIR.iterdir() if d.is_dir()]
    
    for workbook_folder in workbook_folders:
        # Look for processed_pipeline_output.json in each subfolder
        json_file = workbook_folder / "processed_pipeline_output.json"
        
        if not json_file.exists():
            print(f"Warning: processed_pipeline_output.json not found in {workbook_folder.name}")
            continue
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Extract connections from JSON
            connections = get_nested(json_data, "connections", default=[])
            
            for connection in connections:
                conn_name = get_nested(connection, "name", default=None)
               
                if not conn_name or conn_name == "":
                    dataset = get_nested(connection, "dataset", default=None)
                    db_type = get_nested(connection, "type", default="Unknown")
                    if dataset:
                        conn_name = f"{dataset} ({db_type})"
                    else:
                        conn_name = f"Connection ({db_type})" if db_type != "Unknown" else "Unnamed Connection"
                
                db_type = get_nested(connection, "type", default="Unknown")
                conn_id = get_nested(connection, "datasource_id", default=None)
                
                # Determine if published or embedded (if datasource_id exists, consider it published)
                if conn_id:
                    source_type = "Published"
                else:
                    source_type = "Embedded"
                
                source_counter[conn_name]["count"] += 1
                source_counter[conn_name]["db_type"] = db_type
                source_counter[conn_name]["type"] = source_type
        except Exception as e:
            print(f"Error processing {workbook_folder.name}: {e}")

    # Format as DataFrame
    df_sources = pd.DataFrame([
        {
            "Tableau Source": name,
            "Type": data["type"],
            "DB Type": data["db_type"],
            "Usage": data["count"]
        }
        for name, data in source_counter.items()
    ])

    # Convert DB Type to title case with spaces
    def to_title_with_space(text):
        if not text:
            return ""
        return text.replace('_', ' ').replace('-', ' ').title()

    df_sources["DB Type"] = df_sources["DB Type"].apply(to_title_with_space)

    return df_sources
