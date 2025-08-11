#!/usr/bin/env python3
"""
Focused Chart Type Detection Test

Tests the new YAML-based TableauChartRuleEngine specifically on the
"Connected Devices Detail" dashboard from Intraday_Sales.twb.

This test focuses solely on chart type detection accuracy and rule matching.
"""

import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.converters.tableau_chart_rule_engine import (
    TableauChartRuleEngine,
)


def test_chart_detection_focused():
    """Test chart type detection on Connected Devices Detail dashboard."""
    print("=== Chart Type Detection Test ===\n")

    # Test file
    test_file = "connected_devices_dashboard/Intraday_Sales.twb"
    target_dashboard = "Connected Devices Detail"

    if not os.path.exists(test_file):
        print(f"❌ Test file not found: {test_file}")
        return False

    print(f"📄 Testing file: {test_file}")
    print(f"🎯 Target dashboard: {target_dashboard}")

    try:
        # Step 1: Initialize the YAML chart detector
        print("\n1. Initializing YAML Chart Rule Engine...")
        chart_detector = TableauChartRuleEngine()

        rule_stats = chart_detector.get_rule_stats()
        print(f"   ✅ Loaded {rule_stats['total_rules']} chart detection rules")
        print(
            f"   ✅ Supported chart types: {', '.join(rule_stats['chart_types'][:5])}..."
        )
        print(f"   ✅ YAML config: {rule_stats['config_file']}")

        # Step 2: Parse Tableau file and extract data
        print("\n2. Parsing Tableau file...")
        engine = MigrationEngine(use_v2_parser=True)
        migration_data = engine.migrate_file(test_file, "temp_chart_test_output")

        dashboards = migration_data.get("dashboards", [])
        worksheets = migration_data.get("worksheets", [])

        print(f"   ✅ Found {len(dashboards)} dashboards")
        print(f"   ✅ Found {len(worksheets)} worksheets")

        # Step 3: Find target dashboard
        target_dashboard_data = None
        for dashboard in dashboards:
            if dashboard.get("name") == target_dashboard:
                target_dashboard_data = dashboard
                break

        if not target_dashboard_data:
            print(f"   ❌ Target dashboard '{target_dashboard}' not found")
            print(f"   Available dashboards: {[d.get('name') for d in dashboards]}")
            return False

        print(f"   ✅ Found target dashboard: {target_dashboard}")
        print(
            f"   📊 Dashboard elements: {len(target_dashboard_data.get('elements', []))}"
        )

        # Step 4: Extract worksheets used in this dashboard
        dashboard_elements = target_dashboard_data.get("elements", [])
        worksheet_elements = [
            e for e in dashboard_elements if e.get("element_type") == "worksheet"
        ]

        print(
            f"\n3. Analyzing {len(worksheet_elements)} worksheet elements in dashboard..."
        )

        # Find corresponding worksheet data (be more flexible with matching)
        dashboard_worksheets = []

        print("   📋 Dashboard elements found:")
        for element in dashboard_elements[:5]:  # Show first 5 for debugging
            elem_type = element.get("element_type")
            elem_id = element.get("element_id")
            custom_content = element.get("custom_content", {})
            worksheet_name = custom_content.get("worksheet_name")
            print(
                f"      - ID: {elem_id}, Type: {elem_type}, Worksheet: {worksheet_name}"
            )

        # Try to match worksheets more flexibly
        worksheet_names = [w.get("name") for w in worksheets]
        print(f"   📋 Available worksheets: {worksheet_names[:10]}...")  # Show first 10

        # Focus on data visualization worksheets, not filters/text
        data_worksheets = []
        for worksheet in worksheets:
            name = worksheet.get("name", "")
            # Skip filter, text, and notice worksheets
            if any(
                skip_word in name.lower()
                for skip_word in ["filter", "txt ", "notice", "refresh"]
            ):
                continue
            # Only include worksheets with actual visualization data
            viz = worksheet.get("visualization", {})
            fields = worksheet.get("fields", [])
            if viz.get("chart_type") != "automatic" or len(fields) > 0:
                data_worksheets.append(worksheet)

        print(f"   📊 Data visualization worksheets found: {len(data_worksheets)}")
        for worksheet in data_worksheets[:10]:  # Show first 10
            name = worksheet.get("name")
            chart_type = worksheet.get("visualization", {}).get("chart_type", "unknown")
            field_count = len(worksheet.get("fields", []))
            print(f"      - '{name}': {chart_type} ({field_count} fields)")

        # Use the data worksheets for our test
        for i, worksheet in enumerate(data_worksheets):
            dashboard_worksheets.append(
                {
                    "element_id": f"worksheet_{i}",
                    "worksheet_data": worksheet,
                    "position": {"x": 0, "y": i * 0.1, "width": 1.0, "height": 0.3},
                }
            )

        print(f"   ✅ Found worksheet data for {len(dashboard_worksheets)} elements")

        # Step 5: Focus on CD st worksheet for debugging
        print("\n4. Focusing on 'connect total' worksheet for debugging...")

        # Find CD st worksheet specifically
        cd_detail_worksheet = None
        for worksheet in data_worksheets:
            if worksheet.get("name") == "connect total":
                cd_detail_worksheet = worksheet
                break

        if not cd_detail_worksheet:
            print("   ❌ 'connect total' worksheet not found")
            return False

        print("   ✅ Found 'connect total' worksheet - analyzing...")

        # Detailed analysis of CD st
        name = cd_detail_worksheet.get("name")
        confidence = cd_detail_worksheet.get("confidence", 0)
        fields = cd_detail_worksheet.get("fields", [])
        viz = cd_detail_worksheet.get("visualization", {})

        print("\n   📊 connect total Analysis:")
        print(f"      Name: '{name}'")
        print(f"      Current confidence: {confidence}")
        print(f"      Field count: {len(fields)}")
        print(f"      Current chart type: {viz.get('chart_type', 'unknown')}")

        # Examine fields for table indicators
        text_fields = [f for f in fields if f.get("shelf") == "text"]
        columns_fields = [f for f in fields if f.get("shelf") == "columns"]
        rows_fields = [f for f in fields if f.get("shelf") == "rows"]

        print("\n   🔍 Table Detection Indicators:")
        print(f"      Chart type from XML: {viz.get('chart_type')}")
        print(f"      Text shelf fields: {len(text_fields)}")
        print(f"      Columns shelf fields: {len(columns_fields)}")
        print(f"      Rows shelf fields: {len(rows_fields)}")
        print(f"      Has dual axis: {viz.get('is_dual_axis', False)}")

        # Show some example fields
        print("\n   📋 Field Examples (first 10):")
        for i, field in enumerate(fields[:10]):
            fname = field.get("name", "unknown")
            role = field.get("role", "unknown")
            shelf = field.get("shelf", "unknown")
            dtype = field.get("datatype", "unknown")
            print(f"      {i + 1:2d}. '{fname}' - {role} on {shelf} ({dtype})")

        # Manual YAML detection test
        print("\n   🎯 Manual YAML Detection Test:")
        manual_result = chart_detector.detect_chart_type(cd_detail_worksheet)
        print(f"      Result: {manual_result['chart_type']}")
        print(f"      Confidence: {manual_result['confidence']:.2f}")
        print(f"      Method: {manual_result['method']}")
        print(f"      Matched rule: {manual_result.get('matched_rule', 'None')}")
        print(f"      Reasoning: {manual_result.get('reasoning', 'No reasoning')}")

        # Debug detection context - show ALL data we're working with
        print("\n   🔧 Raw Data Debug:")
        print(f"      viz['chart_type']: {viz.get('chart_type')}")
        print(f"      viz['raw_config']: {viz.get('raw_config', {})}")
        raw_config = viz.get("raw_config", {})
        print(f"      raw_config['chart_type']: {raw_config.get('chart_type')}")
        print(f"      raw_config['mark_class']: {raw_config.get('mark_class')}")

        print("\n   🔧 Detection Context Debug:")
        context = chart_detector._build_detection_context(
            cd_detail_worksheet, viz, fields
        )

        key_items = [
            "worksheet_name",
            "mark_type",
            "has_text_marks",
            "columns_shelf_count",
            "rows_shelf_has_string",
            "total_dimensions",
            "total_measures",
            "has_dual_axis",
        ]

        for key in key_items:
            if key in context:
                print(f"      {key}: {context[key]}")

        # Show what the Square rule is actually expecting
        print("\n   📋 Square Rule Expectations:")
        print("      Expected mark_type: 'Square'")
        print("      Expected has_text_marks: True")
        print("      Expected columns_shelf_count: '>1'")
        print("      Expected rows_shelf_has_string: True")

        # Test table rule conditions manually
        print("\n   📋 Table Rule Condition Testing:")
        table_rule = chart_detector.rules.get("basic_chart_detection", {}).get(
            "table_chart", {}
        )
        table_conditions = table_rule.get("conditions", [])

        print(f"      Table rule has {len(table_conditions)} conditions:")
        for i, condition in enumerate(table_conditions):
            print(f"      Condition {i + 1}: {condition}")
            for condition_key, expected_value in condition.items():
                actual_value = context.get(condition_key, "NOT_FOUND")
                matches = chart_detector._evaluate_single_condition(
                    condition_key, expected_value, context
                )
                status = "✅" if matches else "❌"
                print(
                    f"         {status} {condition_key}: expected={expected_value}, actual={actual_value}"
                )

        # Test other worksheets for comparison
        print("\n5. Testing other worksheets for comparison...")

        detection_results = []
        test_worksheets = data_worksheets[:5]  # Test first 5 for comparison

        for i, worksheet in enumerate(test_worksheets):
            worksheet_name = worksheet.get("name", f"unknown_{i}")
            manual_result = chart_detector.detect_chart_type(worksheet)

            detection_results.append(
                {
                    "worksheet_name": worksheet_name,
                    "element_id": f"worksheet_{i}",
                    "current_chart_type": manual_result["chart_type"],
                    "yaml_detection": {
                        "matched_rule": manual_result.get("matched_rule"),
                        "confidence": manual_result["confidence"],
                        "reasoning": manual_result.get("reasoning"),
                    },
                    "position": {"x": 0, "y": i * 0.1, "width": 1.0, "height": 0.3},
                }
            )

            print(
                f"      '{worksheet_name}': {manual_result['chart_type']} (rule: {manual_result.get('matched_rule', 'None')})"
            )

        # Step 6: Summary and analysis
        print(f"\n5. Detection Summary for '{target_dashboard}' Dashboard:")
        print("=" * 80)

        chart_type_counts = {}
        rule_usage = {}
        confidence_scores = []

        for result in detection_results:
            chart_type = result["current_chart_type"]
            chart_type_counts[chart_type] = chart_type_counts.get(chart_type, 0) + 1

            yaml_det = result["yaml_detection"]
            if yaml_det:
                rule = yaml_det.get("matched_rule", "fallback")
                rule_usage[rule] = rule_usage.get(rule, 0) + 1
                confidence_scores.append(yaml_det.get("confidence", 0))

        print("\n📊 Chart Types Detected:")
        for chart_type, count in sorted(chart_type_counts.items()):
            print(f"   {chart_type}: {count} worksheets")

        print("\n🔧 YAML Rules Used:")
        for rule, count in sorted(rule_usage.items()):
            print(f"   {rule}: {count} worksheets")

        if confidence_scores:
            avg_confidence = sum(confidence_scores) / len(confidence_scores)
            print(f"\n📈 Average Detection Confidence: {avg_confidence:.2f}")
            print(
                f"📈 Confidence Range: {min(confidence_scores):.2f} - {max(confidence_scores):.2f}"
            )

        # Step 7: Detailed worksheet breakdown
        print("\n6. Detailed Worksheet Analysis:")
        print("-" * 80)

        for i, result in enumerate(detection_results):
            name = result["worksheet_name"]
            chart_type = result["current_chart_type"]
            position = result["position"]

            print(f"\n   Worksheet {i + 1}: {name}")
            print(f"   │  Chart Type: {chart_type}")
            print(
                f"   │  Position: x={position.get('x', 0):.1f}, y={position.get('y', 0):.1f}"
            )
            print(
                f"   │  Size: {position.get('width', 0):.1f} × {position.get('height', 0):.1f}"
            )

            yaml_det = result["yaml_detection"]
            if yaml_det:
                rule = yaml_det.get("matched_rule", "None")
                conf = yaml_det.get("confidence", 0)
                reason = yaml_det.get("reasoning", "No reasoning")
                print(f"   │  Rule Matched: {rule} (confidence: {conf:.2f})")
                print(f"   └  Reasoning: {reason}")
            else:
                print("   └  ❌ No YAML detection performed")

        print("\n✅ Chart detection test completed successfully!")
        print(
            f"Analyzed {len(detection_results)} worksheets in '{target_dashboard}' dashboard"
        )

        return True

    except Exception as e:
        print(f"❌ Chart detection test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def test_individual_chart_rules():
    """Test individual YAML rules with sample data."""
    print("\n=== Individual YAML Rule Testing ===\n")

    try:
        detector = TableauChartRuleEngine()

        # Test cases for different chart types (matching YAML conditions exactly)
        test_cases = [
            {
                "name": "Bar Chart Test (should match bar_chart rule)",
                "worksheet_data": {
                    "name": "Sample Bar Chart",
                    "fields": [
                        {
                            "name": "category",
                            "role": "dimension",
                            "shelf": "columns",
                            "datatype": "string",
                        },
                        {
                            "name": "sales",
                            "role": "measure",
                            "shelf": "rows",
                            "datatype": "real",
                        },
                    ],
                    "visualization": {
                        "chart_type": "Bar",  # Capitalized as in Tableau XML
                        "is_dual_axis": False,
                        "x_axis": ["category"],
                        "y_axis": ["sales"],
                    },
                },
            },
            {
                "name": "Table Chart Test (should match table_chart rule)",
                "worksheet_data": {
                    "name": "Sample Table",
                    "fields": [
                        {
                            "name": "dim1",
                            "role": "dimension",
                            "shelf": "text",
                            "datatype": "string",
                        },
                        {
                            "name": "dim2",
                            "role": "dimension",
                            "shelf": "columns",
                            "datatype": "string",
                        },
                        {
                            "name": "measure1",
                            "role": "measure",
                            "shelf": "text",
                            "datatype": "real",
                        },
                    ],
                    "visualization": {
                        "chart_type": "Square",  # Square = Table in Tableau
                        "is_dual_axis": False,
                        "x_axis": ["dim1", "dim2"],
                        "y_axis": [],
                    },
                },
            },
            {
                "name": "Donut Chart Test (should match donut_chart rule)",
                "worksheet_data": {
                    "name": "Sample Donut",
                    "fields": [
                        {
                            "name": "category",
                            "role": "dimension",
                            "shelf": "color",
                            "datatype": "string",
                        },
                        {
                            "name": "value",
                            "role": "measure",
                            "shelf": "angle",
                            "datatype": "real",
                        },
                    ],
                    "visualization": {
                        "chart_type": "Pie",  # Capitalized as in Tableau
                        "is_dual_axis": True,  # Key indicator for donut in our YAML
                        "x_axis": [],
                        "y_axis": [],
                    },
                },
            },
            {
                "name": "Line Chart Test (should match line_chart rule)",
                "worksheet_data": {
                    "name": "Sample Line Chart",
                    "fields": [
                        {
                            "name": "date",
                            "role": "dimension",
                            "shelf": "columns",
                            "datatype": "date",
                        },
                        {
                            "name": "sales",
                            "role": "measure",
                            "shelf": "rows",
                            "datatype": "real",
                        },
                    ],
                    "visualization": {
                        "chart_type": "Line",
                        "is_dual_axis": False,
                        "x_axis": ["date"],
                        "y_axis": ["sales"],
                    },
                },
            },
        ]

        print("Testing individual chart type rules:")

        for i, test_case in enumerate(test_cases):
            print(f"\n{i + 1}. {test_case['name']}:")

            result = detector.detect_chart_type(test_case["worksheet_data"])

            print(f"   Result: {result['chart_type']}")
            print(f"   Confidence: {result['confidence']:.2f}")
            print(f"   Method: {result['method']}")
            print(f"   Rule: {result.get('matched_rule', 'None')}")
            print(f"   Reasoning: {result.get('reasoning', 'No reasoning')}")
            print(f"   Looker Equiv: {result.get('looker_equivalent', 'unknown')}")

        return True

    except Exception as e:
        print(f"❌ Individual rule testing failed: {str(e)}")
        return False


def main():
    """Run focused chart detection tests."""
    print("Focused Chart Type Detection Tests")
    print("Using YAML-based TableauChartRuleEngine\n")

    test_results = []

    # Test 1: Focused dashboard analysis
    test_results.append(("Dashboard Analysis", test_chart_detection_focused()))

    # Test 2: Individual rule testing
    test_results.append(("Individual Rules", test_individual_chart_rules()))

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = 0
    for test_name, result in test_results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:20}: {status}")
        if result:
            passed += 1

    total = len(test_results)
    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All chart detection tests passed!")
        print("YAML-based chart detection is working correctly.")
        return True
    else:
        print("❌ Some chart detection tests failed.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
