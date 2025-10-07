#!/usr/bin/env python3
"""
Script to check for duplicate field names in a LookML view file.
"""

import re
import sys
from collections import Counter
from pathlib import Path


def check_duplicates_in_view(file_path):
    """
    Check for duplicate field names and missing field references in a LookML view file.

    Args:
        file_path (str): Path to the .view.lkml file

    Returns:
        dict: Dictionary with duplicate field information and missing references
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

    # Extract field names using regex
    # Pattern matches: dimension: field_name, measure: field_name, parameter: field_name
    pattern = r"^\s*(dimension|measure|parameter):\s+(\w+)"
    matches = re.findall(pattern, content, re.MULTILINE)

    if not matches:
        print("No fields found in the file.")
        return None

    field_names = [match[1] for match in matches]
    # field_types = [match[0] for match in matches]

    # Get line numbers for each field
    lines = content.split("\n")
    field_info = []

    for i, line in enumerate(lines, 1):
        match = re.match(r"^\s*(dimension|measure|parameter):\s+(\w+)", line)
        if match:
            field_type, field_name = match.groups()
            field_info.append({"name": field_name, "type": field_type, "line": i})

    # Check for duplicates
    field_counts = Counter(field_names)
    duplicates = {name: count for name, count in field_counts.items() if count > 1}

    # Check for field references in SQL statements
    defined_fields = set(field_names)
    missing_references = []

    # Special LookML variables that don't need to be defined as fields
    special_variables = {
        "TABLE",  # Base table reference
        "sql_table_name",  # Table name reference
        "sql",  # SQL reference
        "sql_table_name",  # Table name reference
    }

    # Pattern to find ${field_name} references in SQL
    sql_ref_pattern = r"\$\{(\w+)\}"

    for i, line in enumerate(lines, 1):
        # Look for lines that contain SQL statements
        if "sql:" in line.lower() or "sql_table_name:" in line.lower():
            # Find all field references in this line
            refs = re.findall(sql_ref_pattern, line)
            for ref in refs:
                # Skip special LookML variables and fields that are defined
                if ref not in defined_fields and ref not in special_variables:
                    missing_references.append(
                        {"field": ref, "line": i, "context": line.strip()}
                    )

    return {
        "total_fields": len(field_names),
        "unique_fields": len(set(field_names)),
        "duplicates": duplicates,
        "field_info": field_info,
        "missing_references": missing_references,
    }


def main():
    if len(sys.argv) != 2:
        print("Usage: python check_duplicates.py <path_to_view.lkml>")
        print(
            "Example: python check_duplicates.py verizon_test_output/setupgo_test.view.lkml"
        )
        sys.exit(1)

    file_path = sys.argv[1]

    if not Path(file_path).exists():
        print(f"Error: File '{file_path}' does not exist.")
        sys.exit(1)

    print(f"Checking for duplicates in: {file_path}")
    print("=" * 50)

    result = check_duplicates_in_view(file_path)

    if result is None:
        sys.exit(1)

    print(f"Total fields: {result['total_fields']}")
    print(f"Unique fields: {result['unique_fields']}")
    print()

    if result["duplicates"]:
        print("🚨 DUPLICATE FIELDS FOUND:")
        print("-" * 30)

        for field_name, count in result["duplicates"].items():
            print(f"\nField '{field_name}' appears {count} times:")

            # Find all occurrences of this field
            occurrences = [
                info for info in result["field_info"] if info["name"] == field_name
            ]
            for occurrence in occurrences:
                print(
                    f"  - Line {occurrence['line']}: {occurrence['type']}: {occurrence['name']}"
                )
    else:
        print("✅ No duplicate fields found!")

    print()

    if result["missing_references"]:
        print("🚨 MISSING FIELD REFERENCES FOUND:")
        print("-" * 40)

        for ref in result["missing_references"]:
            print(f"\nField '{ref['field']}' is referenced but not defined:")
            print(f"  - Line {ref['line']}: {ref['context']}")
    else:
        print("✅ All field references are properly defined!")


if __name__ == "__main__":
    main()
