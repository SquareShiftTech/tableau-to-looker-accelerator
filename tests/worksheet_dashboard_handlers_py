#!/usr/bin/env python3
"""
Test script for Phase 3 Handlers (WorksheetHandler, DashboardHandler)

Tests the new handlers with raw XML parser output from Bar_charts.twb
"""

import json
import sys
from pathlib import Path

from tableau_to_looker_parser.core.xml_parser_v2 import TableauXMLParserV2
from tableau_to_looker_parser.handlers.worksheet_handler import WorksheetHandler
from tableau_to_looker_parser.handlers.dashboard_handler import DashboardHandler


def test_worksheet_handler():
    """Test WorksheetHandler with real data from Bar_charts.twb"""
    print("🔄 Testing WorksheetHandler")
    print("=" * 40)

    # Parse Bar_charts.twb to get raw worksheet data
    parser = TableauXMLParserV2()
    sample_file = Path("sample_twb_files/Bar_charts.twb")

    if not sample_file.exists():
        print(f"❌ Sample file not found: {sample_file}")
        assert False, f"Sample file not found: {sample_file}"

    try:
        # Get raw worksheet data
        root = parser.parse_file(sample_file)
        raw_worksheets = parser.extract_worksheets(root)

        if not raw_worksheets:
            print("❌ No worksheets found in sample file")
            assert False, "No worksheets found in sample file"

        # Test handler with first few worksheets
        handler = WorksheetHandler()
        processed_worksheets = []

        for i, raw_worksheet in enumerate(raw_worksheets[:3]):  # Test first 3
            print(f"\n📊 Testing worksheet {i + 1}: {raw_worksheet['name']}")

            # Check if handler can process this data
            confidence = handler.can_handle(raw_worksheet)
            print(f"   Handler confidence: {confidence:.2f}")

            if confidence > 0:
                # Process worksheet
                processed = handler.convert_to_json(raw_worksheet)
                processed_worksheets.append(processed)

                print("   ✅ Processed successfully")
                print(f"   - Name: {processed['name']}")
                print(f"   - Clean name: {processed['clean_name']}")
                print(f"   - Datasource: {processed['datasource_id']}")
                print(f"   - Fields: {len(processed['fields'])}")
                print(f"   - Chart type: {processed['visualization']['chart_type']}")
                print(f"   - Processing confidence: {processed['confidence']:.2f}")

                # Show sample fields
                if processed["fields"]:
                    print("   - Sample fields:")
                    for j, field in enumerate(processed["fields"][:2]):
                        print(
                            f"     {j + 1}. {field['name']} ({field['role']}) on {field['shelf']}"
                        )

                # Check for parsing errors
                if processed.get("parsing_errors"):
                    print(f"   ⚠️  Parsing errors: {processed['parsing_errors']}")
            else:
                print("   ❌ Handler cannot process this worksheet")

        # Save sample processed data
        if processed_worksheets:
            output_file = Path("test_worksheet_handler_output.json")
            with open(output_file, "w") as f:
                json.dump(processed_worksheets, f, indent=2)
            print(f"\n💾 Sample processed worksheets saved to: {output_file}")

        print("\n✅ WorksheetHandler test PASSED")
        print(f"   - Tested {len(processed_worksheets)} worksheets")
        print(
            f"   - Average confidence: {sum(w['confidence'] for w in processed_worksheets) / len(processed_worksheets):.2f}"
        )

    except Exception as e:
        print(f"❌ WorksheetHandler test failed: {e}")
        import traceback

        traceback.print_exc()
        assert False, "Test failed"


def test_dashboard_handler():
    """Test DashboardHandler with real data from Bar_charts.twb"""
    print("\n🔄 Testing DashboardHandler")
    print("=" * 40)

    # Parse Bar_charts.twb to get raw dashboard data
    parser = TableauXMLParserV2()
    sample_file = Path("sample_twb_files/Bar_charts.twb")

    if not sample_file.exists():
        print(f"❌ Sample file not found: {sample_file}")
        assert False, f"Sample file not found: {sample_file}"

    try:
        # Get raw dashboard data
        root = parser.parse_file(sample_file)
        raw_dashboards = parser.extract_dashboards(root)

        if not raw_dashboards:
            print("❌ No dashboards found in sample file")
            assert False, "Test failed"

        # Test handler with first dashboard
        handler = DashboardHandler()
        processed_dashboards = []

        for i, raw_dashboard in enumerate(raw_dashboards[:2]):  # Test first 2
            print(f"\n🖥️  Testing dashboard {i + 1}: {raw_dashboard['name']}")

            # Check if handler can process this data
            confidence = handler.can_handle(raw_dashboard)
            print(f"   Handler confidence: {confidence:.2f}")

            if confidence > 0:
                # Process dashboard
                processed = handler.convert_to_json(raw_dashboard)
                processed_dashboards.append(processed)

                print("   ✅ Processed successfully")
                print(f"   - Name: {processed['name']}")
                print(f"   - Clean name: {processed['clean_name']}")
                print(
                    f"   - Canvas size: {processed['canvas_size']['width']}x{processed['canvas_size']['height']}"
                )
                print(f"   - Elements: {len(processed['elements'])}")
                print(f"   - Layout type: {processed['layout_type']}")
                print(f"   - Processing confidence: {processed['confidence']:.2f}")

                # Show sample elements
                if processed["elements"]:
                    print("   - Sample elements:")
                    for j, element in enumerate(processed["elements"][:3]):
                        pos = element["position"]
                        print(
                            f"     {j + 1}. {element['element_type']} at ({pos['x']:.2f}, {pos['y']:.2f})"
                        )
                        if element["element_type"] == "worksheet":
                            worksheet_name = element.get("custom_content", {}).get(
                                "worksheet_name", "Unknown"
                            )
                            print(f"        Worksheet: {worksheet_name}")

                # Check for parsing errors
                if processed.get("parsing_errors"):
                    print(f"   ⚠️  Parsing errors: {processed['parsing_errors']}")
            else:
                print("   ❌ Handler cannot process this dashboard")

        # Save sample processed data
        if processed_dashboards:
            output_file = Path("test_dashboard_handler_output.json")
            with open(output_file, "w") as f:
                json.dump(processed_dashboards, f, indent=2)
            print(f"\n💾 Sample processed dashboards saved to: {output_file}")

        print("\n✅ DashboardHandler test PASSED")
        print(f"   - Tested {len(processed_dashboards)} dashboards")
        print(
            f"   - Average confidence: {sum(d['confidence'] for d in processed_dashboards) / len(processed_dashboards):.2f}"
        )

    except Exception as e:
        print(f"❌ DashboardHandler test failed: {e}")
        import traceback

        traceback.print_exc()
        assert False, "Test failed"


def test_integration():
    """Test handlers integration with plugin registry"""
    print("\n🔄 Testing Plugin Registry Integration")
    print("=" * 45)

    try:
        from tableau_to_looker_parser.core.plugin_registry import PluginRegistry

        # Create plugin registry and register handlers
        registry = PluginRegistry()
        worksheet_handler = WorksheetHandler()
        dashboard_handler = DashboardHandler()

        registry.register_handler(worksheet_handler, priority=7)
        registry.register_handler(dashboard_handler, priority=8)

        print("✅ Handlers registered successfully")

        # Test with sample data
        sample_worksheet_data = {
            "name": "Test_Worksheet",
            "datasource_id": "test_ds",
            "fields": [{"name": "category", "role": "dimension", "shelf": "rows"}],
            "visualization": {"chart_type": "bar", "x_axis": [], "y_axis": []},
        }

        sample_dashboard_data = {
            "name": "Test_Dashboard",
            "canvas_size": {"width": 1000, "height": 800},
            "elements": [
                {
                    "element_id": "1",
                    "element_type": "worksheet",
                    "position": {"x": 0, "y": 0, "width": 1, "height": 1},
                }
            ],
        }

        # Test handler selection
        worksheet_handler_found = registry.get_handler(sample_worksheet_data)
        dashboard_handler_found = registry.get_handler(sample_dashboard_data)

        if isinstance(worksheet_handler_found, WorksheetHandler):
            print("✅ WorksheetHandler correctly selected for worksheet data")
        else:
            print("❌ WorksheetHandler not selected for worksheet data")
            assert False, "Test failed"

        if isinstance(dashboard_handler_found, DashboardHandler):
            print("✅ DashboardHandler correctly selected for dashboard data")
        else:
            print("❌ DashboardHandler not selected for dashboard data")
            assert False, "Test failed"

        print("\n✅ Plugin Registry Integration test PASSED")

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback

        traceback.print_exc()
        assert False, "Test failed"


def main():
    """Run all handler tests"""
    print("🧪 Testing Phase 3 Handlers")
    print("=" * 50)

    results = []

    # Test individual handlers
    results.append(test_worksheet_handler())
    results.append(test_dashboard_handler())
    results.append(test_integration())

    # Summary
    passed = sum(results)
    total = len(results)

    print("\n📊 Test Summary")
    print("=" * 20)
    print(f"Tests passed: {passed}/{total}")

    if passed == total:
        print("🎉 All Phase 3 Handler tests PASSED!")
    else:
        print("❌ Some tests failed")
        assert False, "Test failed"


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
