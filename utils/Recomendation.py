import os
import json
from pathlib import Path
import pandas as pd
import re
from google.cloud import bigquery
from fuzzywuzzy import process
from dotenv import load_dotenv
from utils.Complexity_Analysis import detect_features_from_json, load_json_file

def get_nested(data, *keys, default=None):
    """Helper function to safely get nested values from JSON."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, default)
        else:
            return default
        if data is None:
            return default
    return data

def normalize(text):
    return re.sub(r'[^a-z0-9]', '', str(text).lower().strip())

def recommendation():
    load_dotenv()
    # Resolve WORKBOOK_DIR relative to the project root
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    WORKBOOK_DIR = project_root / "output" / "workbook"

    PROJECT_ID = os.getenv("PROJECT_ID")
    BQ_TABLE = os.getenv("DATASET_TABLE")

    if not WORKBOOK_DIR.exists():
        print(f"Warning: Workbook directory {WORKBOOK_DIR} does not exist.")
        WORKBOOK_DIR.mkdir(parents=True, exist_ok=True)
        return pd.DataFrame(columns=["Complexity", "Description", "Feature", "Reason", "Recommended Approach", "Workbook", "Dashboard Name"])

    # --- Dashboard-level feature extraction from JSON: one row per (feature, workbook, dashboard) ---
    results = []
    # Iterate through each subfolder in workbook directory
    workbook_folders = [d for d in WORKBOOK_DIR.iterdir() if d.is_dir()]
    
    if not workbook_folders:
        print(f"Warning: No workbook folders found in {WORKBOOK_DIR}")
        return pd.DataFrame(columns=["Complexity", "Description", "Feature", "Reason", "Recommended Approach", "Workbook", "Dashboard Name"])
    
    print(f"Processing {len(workbook_folders)} workbook folder(s) from {WORKBOOK_DIR}")
    
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
            
            workbook_name = workbook_folder.name  # Use folder name as workbook name
            
            # Extract dashboards from JSON
            dashboard_list = get_nested(json_data, "dashboards", default=[])
            # if not isinstance(dashboard_list, list):
            #     dashboard_list = [dashboard_list] if dashboard_list else []
            
            # Process each dashboard individually
            for dashboard in dashboard_list:
                dashboard_name = get_nested(dashboard, "name", default="")
                if not dashboard_name:
                    continue
                
                # Create a JSON structure that includes this dashboard plus workbook-level data
                # This ensures we detect both workbook-level features (tables, connections, etc.)
                # and dashboard-level features (charts, filters, etc.)
                dashboard_json_with_context = {
                    "dashboards": [dashboard],
                    "tables": json_data.get("tables", []),
                    "relationships": json_data.get("relationships", []),
                    "connections": json_data.get("connections", []),
                    "calculated_fields": json_data.get("calculated_fields", []),
                    "parameters": json_data.get("parameters", []),
                    "actions": json_data.get("actions", [])
                }
                
                # Detect features for this specific dashboard (with workbook context)
                flags = detect_features_from_json(dashboard_json_with_context)
                for feature, present in flags.items():
                    if present:
                        results.append({
                            "Feature": feature,
                            "Workbook": workbook_name,
                            "Dashboard Name": dashboard_name
                        })
        except Exception as e:
            print(f"Error processing {workbook_folder.name}: {e}")

    # --- Create DataFrame from results ---
    if not results:
        print("Warning: No features detected in any dashboards.")
        return pd.DataFrame(columns=["Complexity", "Description", "Feature", "Reason", "Recommended Approach", "Workbook", "Dashboard Name"])
    
    df = pd.DataFrame(results)
    print(f"Detected {len(df)} feature occurrences across all dashboards")

    # ---- Load Metadata from BigQuery ----
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT Features, Complexity, Reason, `Recommended_Approach`, Description FROM `{BQ_TABLE}`"
    metadata_df = bq_client.query(query).to_dataframe()
    metadata_df["clean_feature"] = metadata_df["Features"].apply(normalize)
    df["clean_feature"] = df["Feature"].apply(normalize)

    def fuzzy_match(row, choices):
        match, score = process.extractOne(row["clean_feature"], choices)
        return match if score >= 85 else None

    choices = metadata_df["clean_feature"].tolist()
    df["matched_clean_feature"] = df.apply(
        lambda row: fuzzy_match(row, choices), axis=1
    )

    merged_df = pd.merge(
        df,
        metadata_df,
        left_on="matched_clean_feature",
        right_on="clean_feature",
        how="left"
    )

    final_df = merged_df[[
        "Feature", "Workbook", "Dashboard Name", "Complexity", "Reason", "Recommended_Approach", "Description"
    ]]
    final_df = final_df.dropna(subset=["Reason", "Recommended_Approach"])
    # Dashboard Name should always be present since we process from JSON, but keep filter for safety
    final_df = final_df[final_df["Dashboard Name"].notnull() & (final_df["Dashboard Name"] != "")]

    # Clean workbook name (remove any suffixes if present)
    def clean_workbook_name(wb):
        # JSON filenames don't have .twbx extension, but keep cleaning logic for consistency
        return re.sub(r'(-modified_\d+|_\d+)?(\.twbx|\.json)?$', '', str(wb))
    final_df["Workbook"] = final_df["Workbook"].apply(clean_workbook_name)

    final_df = final_df.rename(columns={"Recommended_Approach": "Recommended Approach"})
    column_order = ["Complexity", "Description", "Feature", "Reason", "Recommended Approach", "Workbook", "Dashboard Name"]
    final_df = final_df[column_order]
        
    def clean_multiline(text):
        if pd.isnull(text):
            return ""
        return str(text).replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').strip()

    for col in ["Description", "Reason", "Recommended Approach"]:
        final_df[col] = final_df[col].apply(clean_multiline)
    # final_df.to_csv("recommendation_analysis_output.csv", index=False)

    return final_df
