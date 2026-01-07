import json
import sys
import os
from glob import glob

def extract_relationships(relationships):
    """Extract only specified fields from relationships"""
    result = []
    for rel in relationships:
        extracted = {
            "relationship_type": rel.get("relationship_type", ""),
            "name": rel.get("name", ""),
            "join_type": rel.get("join_type", ""),
            "table_aliases": rel.get("table_aliases", {"": "", "": ""})
        }
        result.append(extracted)
    return result

def extract_tables(tables):
    """Extract only specified fields from tables"""
    result = []
    for table in tables:
        extracted = {
            "class": table.get("class", ""),
            "connection": table.get("connection", ""),
            "name": table.get("name", ""),
            "table": table.get("table", ""),
            "relation_type": table.get("relation_type", "")
        }
        result.append(extracted)
    return result

def extract_connections(connections):
    """Extract only specified fields from connections"""
    result = []
    for conn in connections:
        extracted = {
            "type": conn.get("type", ""),
            "dataset": conn.get("dataset") or conn.get("database") or conn.get("name", "")
        }
        result.append(extracted)
    return result

def extract_field(field):
    """Extract only name and datatype from field"""
    return {
        "name": field.get("name", ""),
        "datatype": field.get("datatype", "")
    }

def extract_visualization(viz):
    """Extract chart_type, show_labels, is_dual_axis, and raw_config from visualization"""
    if not viz:
        return {
            "chart_type": "",
            "show_labels": False,
            "is_dual_axis": False,
            "raw_config": {}
        }
    result = {
        "chart_type": viz.get("chart_type", ""),
        "show_labels": viz.get("show_labels", False),
        "is_dual_axis": viz.get("is_dual_axis", False)
    }
    # Include raw_config if it exists, but only chart_type and chart_type_extracted fields
    # The raw_config may be nested (raw_config.raw_config) or at top level
    raw_config = viz.get("raw_config", {})
    if raw_config:
        # Extract only chart_type and chart_type_extracted from raw_config
        extracted_raw_config = {}
        
        # Check nested raw_config first
        nested_raw_config = raw_config.get("raw_config", {})
        source_config = nested_raw_config if nested_raw_config else raw_config
        
        # Only extract chart_type and chart_type_extracted
        if "chart_type" in source_config:
            extracted_raw_config["chart_type"] = source_config["chart_type"]
        if "chart_type_extracted" in source_config:
            extracted_raw_config["chart_type_extracted"] = source_config["chart_type_extracted"]
        
        if extracted_raw_config:
            result["raw_config"] = extracted_raw_config
    return result

def extract_parameter(param):
    """Extract only specified fields from parameter"""
    return {
        "name": param.get("name", ""),
        "param_domain_type": param.get("param_domain_type", ""),
        "parameter-type": param.get("parameter-type", "")
    }

def extract_worksheet(worksheet):
    """Extract only specified fields from worksheet"""
    extracted = {
        "name": worksheet.get("name", ""),
        "fields": [extract_field(f) for f in worksheet.get("fields", [])],
        "hierarchy_usage": {
            "has_hierarchy_usage": worksheet.get("hierarchy_usage", {}).get("has_hierarchy_usage", False)
        },
        "cascading_filter": {
            "has_cascading_filter": worksheet.get("cascading_filter", {}).get("has_cascading_filter", False)
        },
        "visualization": extract_visualization(worksheet.get("visualization")),
        "filters": [],
        "groupfilter_logic": [],
        "parameter": []
    }
    
    # Extract filters and groupfilter_logic
    filters = worksheet.get("filters", [])
    all_groupfilter_logic = []
    for filter_item in filters:
        # Extract filter info
        filter_data = {
            "field_name": filter_item.get("field_name", ""),
            "filter_type": filter_item.get("filter_type", "")
        }
        extracted["filters"].append(filter_data)
        
        # Collect all groupfilter_logic entries
        groupfilter = filter_item.get("groupfilter_logic", [])
        if groupfilter:
            all_groupfilter_logic.extend(groupfilter)
    
    # Set groupfilter_logic at worksheet level
    extracted["groupfilter_logic"] = all_groupfilter_logic if all_groupfilter_logic else []
    
    # Extract parameters
    parameters = worksheet.get("parameters", [])
    extracted["parameter"] = [extract_parameter(p) for p in parameters]
    
    return extracted

def extract_dashboard(dashboard, worksheets_dict):
    """Extract only specified fields from dashboard"""
    extracted = {
        "name": dashboard.get("name", ""),
        "worksheet": [],
        "dynamic_toggle": dashboard.get("dynamic_toggle", "")
    }
    
    # Use a dictionary to track unique worksheets by name (to avoid duplicates)
    unique_worksheets = {}
    
    # Find worksheets in dashboard elements
    elements = dashboard.get("elements", [])
    
    # Get worksheets from elements (worksheets are embedded in elements)
    for element in elements:
        if element.get("element_type") == "worksheet":
            # Worksheet data is embedded in the element
            worksheet_data = element.get("worksheet")
            if worksheet_data and isinstance(worksheet_data, dict):
                ws_name = worksheet_data.get("name", "")
                if ws_name and ws_name not in unique_worksheets:
                    unique_worksheets[ws_name] = worksheet_data
            else:
                # Try to find worksheet name from custom_content
                custom_content = element.get("custom_content", {})
                worksheet_name = custom_content.get("worksheet_name") or custom_content.get("name")
                if worksheet_name and worksheet_name in worksheets_dict:
                    if worksheet_name not in unique_worksheets:
                        unique_worksheets[worksheet_name] = worksheets_dict[worksheet_name]
    
    # Also check global_filters and toggles for worksheet references
    for filter_item in dashboard.get("global_filters", []):
        name = filter_item.get("name")
        if name and name in worksheets_dict:
            if name not in unique_worksheets:
                unique_worksheets[name] = worksheets_dict[name]
    
    for toggle in dashboard.get("toggles", []):
        name = toggle.get("name")
        if name and name in worksheets_dict:
            if name not in unique_worksheets:
                unique_worksheets[name] = worksheets_dict[name]
    
    # Extract worksheets (now deduplicated)
    for ws_name, ws_data in unique_worksheets.items():
        extracted["worksheet"].append(extract_worksheet(ws_data))
    
    # If no worksheets found from elements/filters/toggles, check other sources
    if not extracted["worksheet"]:
        # Check if dashboard has worksheets directly in "worksheets" array
        dashboard_worksheets = dashboard.get("worksheets", [])
        seen_names = set()
        for ws in dashboard_worksheets:
            ws_name = ws.get("name", "")
            if ws_name and ws_name not in seen_names:
                extracted["worksheet"].append(extract_worksheet(ws))
                seen_names.add(ws_name)
        
        # Also check if dashboard already has "worksheet" array (already transformed file)
        if not extracted["worksheet"]:
            dashboard_worksheet_array = dashboard.get("worksheet", [])
            seen_names = set()
            for ws in dashboard_worksheet_array:
                if isinstance(ws, dict):
                    ws_name = ws.get("name", "")
                    if ws_name and ws_name not in seen_names:
                        extracted["worksheet"].append(extract_worksheet(ws))
                        seen_names.add(ws_name)
    
    # Final deduplication pass to ensure no duplicates (in case of already-transformed files)
    final_worksheets = []
    seen_names = set()
    for ws in extracted["worksheet"]:
        ws_name = ws.get("name", "")
        if ws_name and ws_name not in seen_names:
            final_worksheets.append(ws)
            seen_names.add(ws_name)
    
    extracted["worksheet"] = final_worksheets
    return extracted

def extract_action(action):
    """Extract only specified fields from action"""
    extracted = {
        "activation": {
            "type": action.get("activation", {}).get("type", "")
        },
        "source": action.get("source", {}),
        "command": {
            "command": action.get("command", {}).get("command", ""),
            "params": action.get("command", {}).get("params", [])
        }
    }
    return extracted

def extract_calculated_field(cf):
    """Extract only specified fields from calculated field"""
    return {
        "name": cf.get("name", ""),
        "calculation_class": cf.get("calculation_class", ""),
        "calculation": {
            "original_formula": cf.get("calculation", {}).get("original_formula", "")
        }
    }

def transform_json(input_file, output_file, quiet=False):
    """
    Transform JSON to extract only specified fields.
    
    How input JSON is passed:
    1. Input file path is passed as 'input_file' parameter (string path to JSON file)
    2. JSON file is read using json.load() which parses the entire JSON structure
    3. The parsed JSON data (Python dict) is stored in 'data' variable
    4. From 'data', we extract specific sections using .get() method:
       - data.get("relationships", []) - gets relationships array or empty list
       - data.get("tables", []) - gets tables array
       - data.get("connections", []) - gets connections array
       - data.get("worksheets", []) - gets worksheets array (used for lookup)
       - data.get("dashboards", []) - gets dashboards array
       - data.get("actions", []) - gets actions array
       - data.get("calculated_fields", []) - gets calculated_fields array
    
    Args:
        input_file (str): Path to the input JSON file
        output_file (str): Path to the output JSON file
        quiet (bool): If True, suppress verbose output (default: False)
    """
    if not quiet:
        print(f"Reading input JSON from: {input_file}")
    
    # Read and parse the input JSON file
    # json.load() reads the file and parses it into a Python dictionary
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)  # 'data' is now a Python dict containing the entire JSON structure
    
    # Display input JSON structure
    if not quiet:
        print("\n=== Input JSON Structure ===")
        print(f"Top-level keys in input JSON: {list(data.keys())}")
        print(f"  - relationships: {len(data.get('relationships', []))} items")
        print(f"  - tables: {len(data.get('tables', []))} items")
        print(f"  - connections: {len(data.get('connections', []))} items")
        print(f"  - worksheets: {len(data.get('worksheets', []))} items")
        print(f"  - dashboards: {len(data.get('dashboards', []))} items")
        print(f"  - actions: {len(data.get('actions', []))} items")
        print(f"  - calculated_fields: {len(data.get('calculated_fields', []))} items")
    
    # Create worksheets dictionary for easy lookup
    worksheets_dict = {}
    for ws in data.get("worksheets", []):
        worksheets_dict[ws.get("name", "")] = ws
    
    if not quiet:
        print(f"\nCreated worksheets dictionary with {len(worksheets_dict)} worksheets")
    
    # Build transformed structure
    if not quiet:
        print("\n=== Transforming Data ===")
        print("Extracting relationships...")
    transformed = {
        "relationships": extract_relationships(data.get("relationships", []))
    }
    
    if not quiet:
        print("Extracting tables...")
    transformed["tables"] = extract_tables(data.get("tables", []))
    
    if not quiet:
        print("Extracting connections...")
    transformed["connections"] = extract_connections(data.get("connections", []))
    
    if not quiet:
        print("Extracting dashboards and worksheets...")
    transformed["dashboards"] = [extract_dashboard(d, worksheets_dict) for d in data.get("dashboards", [])]
    
    if not quiet:
        print("Extracting actions...")
    transformed["actions"] = [extract_action(a) for a in data.get("actions", [])]
    
    if not quiet:
        print("Extracting calculated fields...")
    transformed["calculated_fields"] = [extract_calculated_field(cf) for cf in data.get("calculated_fields", [])]
    
    # Write output
    if not quiet:
        print(f"\nWriting transformed JSON to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(transformed, f, indent=2, ensure_ascii=False)
    
    if not quiet:
        print("\n=== Transformation Summary ===")
        print(f"Output JSON structure:")
        print(f"  - relationships: {len(transformed['relationships'])} items")
        print(f"  - tables: {len(transformed['tables'])} items")
        print(f"  - connections: {len(transformed['connections'])} items")
        print(f"  - dashboards: {len(transformed['dashboards'])} items")
        total_worksheets = sum(len(d.get('worksheet', [])) for d in transformed['dashboards'])
        print(f"    └─ Total worksheets in dashboards: {total_worksheets}")
        print(f"  - actions: {len(transformed['actions'])} items")
        print(f"  - calculated_fields: {len(transformed['calculated_fields'])} items")
        print(f"\n✓ Transformation complete. Output written to {output_file}")

def find_all_json_files(directory="output"):
    """
    Find all processed_pipeline_output.json files in the output directory.
    
    Args:
        directory (str): Root directory to search in
        
    Returns:
        list: List of file paths to JSON files
    """
    json_files = []
    pattern = os.path.join(directory, "**", "processed_pipeline_output.json")
    json_files = glob(pattern, recursive=True)
    return sorted(json_files)

def process_all_json_files(directory="output", overwrite=True):
    """
    Process all JSON files found in the directory.
    
    Args:
        directory (str): Root directory to search for JSON files
        overwrite (bool): If True, overwrite original files. If False, add '_transformed' suffix.
    """
    json_files = find_all_json_files(directory)
    
    if not json_files:
        print(f"No JSON files found in {directory}")
        return
    
    print(f"Found {len(json_files)} JSON file(s) to process:\n")
    for i, json_file in enumerate(json_files, 1):
        print(f"  {i}. {json_file}")
    
    print("\n" + "="*60)
    print("Processing all JSON files...")
    print("="*60 + "\n")
    
    for i, input_file in enumerate(json_files, 1):
        print(f"\n[{i}/{len(json_files)}] Processing: {input_file}")
        print("-" * 60)
        
        if overwrite:
            output_file = input_file
        else:
            # Add _transformed suffix before .json
            base = os.path.splitext(input_file)[0]
            output_file = f"{base}_transformed.json"
        
        try:
            transform_json(input_file, output_file)
        except Exception as e:
            print(f"✗ Error processing {input_file}: {e}")
            continue
    
    print("\n" + "="*60)
    print(f"✓ Completed processing {len(json_files)} file(s)")
    print("="*60)

if __name__ == "__main__":
    """
    Main entry point for the script.
    
    How input JSON is passed:
    1. Default (no args): Processes ALL JSON files in output/ directory
    2. Single file: python transform_json.py input.json [output.json]
    3. Directory: python transform_json.py --dir output [--no-overwrite]
    
    Usage examples:
        python transform_json.py                    # Process all JSON files in output/
        python transform_json.py input.json         # Process single file (overwrite)
        python transform_json.py input.json out.json # Process single file (save to out.json)
        python transform_json.py --dir output       # Process all in output/ directory
        python transform_json.py --dir output --no-overwrite  # Save as _transformed.json
    """
    # Check for directory processing mode
    if len(sys.argv) > 1 and sys.argv[1] == "--dir":
        directory = sys.argv[2] if len(sys.argv) > 2 else "output"
        overwrite = "--no-overwrite" not in sys.argv
        process_all_json_files(directory, overwrite=overwrite)
    elif len(sys.argv) > 1:
        # Single file mode
        input_file = sys.argv[1]
        if len(sys.argv) > 2:
            output_file = sys.argv[2]
        else:
            output_file = input_file  # Overwrite by default
        
        transform_json(input_file, output_file)
    else:
        # Default: Process all JSON files in output directory
        print("No arguments provided. Processing all JSON files in output/ directory...")
        print("(Use 'python transform_json.py --help' for usage information)\n")
        process_all_json_files("output", overwrite=True)

