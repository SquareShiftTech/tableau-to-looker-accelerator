#!/usr/bin/env python3
"""
Test script for the new Looker-native dashboard generation architecture.

Tests the LookerElementGenerator and LookerNativeDashboardGenerator with the
Connected Devices Dashboard data to validate YAML-driven field mapping.
"""

import json
import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tableau_to_looker_parser.generators.looker_element_generator import (
    LookerElementGenerator,
)
from tableau_to_looker_parser.generators.looker_native_dashboard_generator import (
    LookerNativeDashboardGenerator,
)
from tableau_to_looker_parser.models.worksheet_models import WorksheetSchema


def test_element_generation():
    """Test LookerElementGenerator with sample worksheet data."""
    print("=== Testing LookerElementGenerator ===\n")

    # Load processed pipeline data
    with open(
        "comprehensive_dashboard_test_output/processed_pipeline_output.json", "r"
    ) as f:
        migration_data = json.load(f)

    # Initialize element generator
    element_generator = LookerElementGenerator(
        model_name="bigquery_super_store_sales_model",
        explore_name="intradaysales_results_hqa_pd_qmtbls_mock",
    )

    # Test with different worksheet types
    test_worksheets = ["CD detail"]

    for worksheet_name in test_worksheets:
        print(f"📊 Testing worksheet: {worksheet_name}")

        # Find worksheet in data
        worksheet_data = None
        for ws in migration_data.get("worksheets", []):
            if ws.get("name") == worksheet_name:
                worksheet_data = ws
                break

        if not worksheet_data:
            print(f"❌ Worksheet {worksheet_name} not found")
            continue

        try:
            # Convert to schema
            worksheet = WorksheetSchema(**worksheet_data)

            # Sample position for testing
            position = {"row": 5, "col": 0, "width": 12, "height": 6}

            # Generate element
            element = element_generator.generate_element(worksheet, position)

            # Display results
            print("   ✅ Generated element successfully")
            print(f"   Chart Type: {element['type']}")
            print(f"   Fields: {len(element['fields'])} fields")
            print(f"   Pivots: {len(element.get('pivots', []))} pivots")
            print(f"   Sorts: {len(element.get('sorts', []))} sorts")

            # Show YAML detection info if available
            if worksheet.visualization.yaml_detection:
                yaml_info = worksheet.visualization.yaml_detection
                print("   YAML Detection:")
                print(f"     - Confidence: {yaml_info.get('confidence')}")
                print(f"     - Rule: {yaml_info.get('matched_rule')}")
                print(f"     - Looker Type: {yaml_info.get('looker_equivalent')}")
                print(f"     - Pivot Required: {yaml_info.get('pivot_required')}")

            print()

        except Exception as e:
            print(f"❌ Error generating element for {worksheet_name}: {e}")
            print()
            continue


def test_dashboard_generation():
    """Test LookerNativeDashboardGenerator with complete dashboard."""
    print("=== Testing LookerNativeDashboardGenerator ===\n")

    # Load processed pipeline data
    with open(
        "comprehensive_dashboard_test_output/processed_pipeline_output.json", "r"
    ) as f:
        migration_data = json.load(f)

    # Initialize dashboard generator
    dashboard_generator = LookerNativeDashboardGenerator()

    # Test dashboard generation
    try:
        output_dir = "test_looker_native_output"

        print("📄 Generating Looker-native dashboards...")
        generated_files = dashboard_generator.generate(migration_data, output_dir)

        print(f"✅ Generated {len(generated_files)} dashboard files:")
        for file_path in generated_files:
            print(f"   - {file_path}")

            # Show snippet of generated content
            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    content = f.read()
                    lines = content.split("\n")

                print(f"   Content preview ({len(lines)} lines):")
                for i, line in enumerate(lines[:15]):  # Show first 15 lines
                    print(f"     {i + 1:2d}: {line}")
                if len(lines) > 15:
                    print(f"     ... ({len(lines) - 15} more lines)")
                print()

    except Exception as e:
        print(f"❌ Error generating dashboard: {e}")
        import traceback

        traceback.print_exc()


def compare_with_reference():
    """Compare generated output with reference Connected Devices Dashboard."""
    print("=== Comparing with Reference Dashboard ===\n")

    # Load reference dashboard
    reference_path = "connected_devices_dashboard/Intraday_Sales.dashboard.lookml"
    if not os.path.exists(reference_path):
        print(f"❌ Reference file not found: {reference_path}")
        return

    generated_path = "test_looker_native_output"
    generated_files = []

    if os.path.exists(generated_path):
        generated_files = [
            f for f in os.listdir(generated_path) if f.endswith(".dashboard.lookml")
        ]

    if not generated_files:
        print("❌ No generated dashboard files found")
        return

    print("📊 Reference vs Generated Comparison:")

    with open(reference_path, "r") as f:
        reference_content = f.read()

    with open(f"{generated_path}/{generated_files[0]}", "r") as f:
        generated_content = f.read()

    # Basic comparison metrics
    ref_lines = reference_content.count("\n")
    gen_lines = generated_content.count("\n")

    ref_elements = reference_content.count("- title:")
    gen_elements = generated_content.count("- title:")

    print("   Reference Dashboard:")
    print(f"     - Lines: {ref_lines}")
    print(f"     - Elements: {ref_elements}")
    print(
        f"     - Uses: {'ECharts' if 'echarts_visualization_prod' in reference_content else 'Looker-native'}"
    )

    print("   Generated Dashboard:")
    print(f"     - Lines: {gen_lines}")
    print(f"     - Elements: {gen_elements}")
    print(
        f"     - Uses: {'ECharts' if 'echarts_visualization_prod' in generated_content else 'Looker-native'}"
    )

    print(
        f"   Status: {'✅ Looker-native' if 'echarts_visualization_prod' not in generated_content else '❌ Still ECharts'}"
    )


def main():
    """Run all tests for the new Looker-native architecture."""
    print("🚀 Testing Looker-Native Dashboard Generation Architecture\n")

    # Test element generation
    test_element_generation()

    # Test dashboard generation
    test_dashboard_generation()

    # Compare with reference
    compare_with_reference()

    print("🎉 Testing completed!")


if __name__ == "__main__":
    main()
