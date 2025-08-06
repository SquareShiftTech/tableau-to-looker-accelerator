#!/usr/bin/env python3
"""
Test MigrationEngine with complete Phase 3 integration
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tableau_to_looker_parser.core.migration_engine import MigrationEngine


def test_migration_engine_phase3():
    """Test MigrationEngine with Phase 3 worksheet and dashboard integration"""
    print("🔗 Testing MigrationEngine Phase 3 Integration")
    print("=" * 50)

    # Initialize MigrationEngine with v2 parser (required for Phase 3)
    engine = MigrationEngine(use_v2_parser=True)

    sample_file = Path("sample_twb_files/Bar_charts.twb")
    output_dir = Path("test_output")

    if not sample_file.exists():
        print(f"❌ Sample file not found: {sample_file}")
        assert False, f"Sample file not found: {sample_file}"

    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)

    try:
        print(f"📁 Processing: {sample_file}")
        print(f"📂 Output dir: {output_dir}")

        # Run complete migration
        result = engine.migrate_file(str(sample_file), str(output_dir))

        print("\n📊 Migration Results:")
        print(f"   - Tables: {len(result.get('tables', []))}")
        print(f"   - Connections: {len(result.get('connections', []))}")
        print(f"   - Dimensions: {len(result.get('dimensions', []))}")
        print(f"   - Measures: {len(result.get('measures', []))}")
        print(f"   - Calculated fields: {len(result.get('calculated_fields', []))}")
        print(f"   - Worksheets: {len(result.get('worksheets', []))}")
        print(f"   - Dashboards: {len(result.get('dashboards', []))}")

        # Test Phase 3 data quality
        worksheets = result.get("worksheets", [])
        dashboards = result.get("dashboards", [])

        if not worksheets:
            print("   ❌ No worksheets found!")
            assert False, "No worksheets found!"

        if not dashboards:
            print("   ❌ No dashboards found!")
            assert False, "No dashboards found!"

        print("\n📊 Worksheet Analysis:")
        for i, worksheet in enumerate(worksheets[:3]):  # Show first 3
            print(f"   {i + 1}. {worksheet['name']}")
            print(f"      - Chart: {worksheet['visualization']['chart_type']}")
            print(f"      - Fields: {len(worksheet['fields'])}")
            print(f"      - Confidence: {worksheet['confidence']:.2f}")

        print("\n🖥️  Dashboard Analysis:")
        total_elements = 0
        linked_worksheets = 0

        for i, dashboard in enumerate(dashboards):
            elements = dashboard["elements"]
            total_elements += len(elements)

            # Count linked worksheets (should have full worksheet data now)
            worksheet_elements = [
                e for e in elements if e["element_type"] == "worksheet"
            ]
            linked_count = sum(
                1 for e in worksheet_elements if e["worksheet"] is not None
            )
            linked_worksheets += linked_count

            print(f"   {i + 1}. {dashboard['name']}")
            print(f"      - Elements: {len(elements)}")
            print(f"      - Worksheet elements: {len(worksheet_elements)}")
            print(f"      - Linked worksheets: {linked_count}")
            print(
                f"      - Link success: {linked_count / len(worksheet_elements) * 100:.0f}%"
                if worksheet_elements
                else "      - Link success: N/A"
            )

            # Show detailed info for first dashboard
            if i == 0:
                print("      - Element details:")
                for j, elem in enumerate(elements[:3]):  # First 3 elements
                    if elem["element_type"] == "worksheet" and elem["worksheet"]:
                        ws = elem["worksheet"]
                        print(
                            f"        {j + 1}. {elem['element_type']} '{ws['name']}' - {ws['visualization']['chart_type']} chart"
                        )
                        print(
                            f"           Position: ({elem['position']['x']:.2f}, {elem['position']['y']:.2f})"
                        )
                        print(f"           Fields: {len(ws['fields'])}")
                    else:
                        print(
                            f"        {j + 1}. {elem['element_type']} (no worksheet data)"
                        )

        # Overall integration quality
        link_percentage = (
            (linked_worksheets / total_elements * 100) if total_elements > 0 else 0
        )

        print("\n🔗 Integration Quality:")
        print(f"   - Total dashboard elements: {total_elements}")
        print(f"   - Successfully linked worksheets: {linked_worksheets}")
        print(f"   - Overall link success rate: {link_percentage:.1f}%")

        # Check output files
        output_file = output_dir / "processed_pipeline_output.json"
        if output_file.exists():
            file_size = output_file.stat().st_size / 1024  # KB
            print(f"   - Output file: {output_file} ({file_size:.1f} KB)")

        # Validation checks
        success_criteria = []

        # Must have worksheets and dashboards
        success_criteria.append(len(worksheets) > 0)
        success_criteria.append(len(dashboards) > 0)

        # At least 80% of worksheet elements should be linked
        success_criteria.append(link_percentage >= 80)

        # Worksheets should have valid chart types and fields
        valid_worksheets = sum(
            1
            for ws in worksheets
            if ws.get("visualization", {}).get("chart_type")
            and len(ws.get("fields", [])) > 0
        )
        success_criteria.append(valid_worksheets >= len(worksheets) * 0.8)

        passed_criteria = sum(success_criteria)
        total_criteria = len(success_criteria)

        print(f"\n✅ Success Criteria: {passed_criteria}/{total_criteria}")

        if passed_criteria == total_criteria:
            print("🎉 MigrationEngine Phase 3 Integration: EXCELLENT!")
        elif passed_criteria >= total_criteria * 0.75:
            print("✅ MigrationEngine Phase 3 Integration: GOOD")
        else:
            print("❌ MigrationEngine Phase 3 Integration: NEEDS IMPROVEMENT")
            assert passed_criteria >= total_criteria * 0.75, (
                f"Integration test failed: {passed_criteria}/{total_criteria} criteria passed"
            )

    except Exception as e:
        print(f"❌ Migration test failed: {e}")
        import traceback

        traceback.print_exc()
        raise


def main():
    """Run MigrationEngine Phase 3 test"""
    print("🧪 MigrationEngine Phase 3 Integration Test")
    print("=" * 45)

    success = test_migration_engine_phase3()

    print("\n📊 Test Results")
    print("=" * 20)

    if success:
        print("🎉 MigrationEngine Phase 3 integration test PASSED!")
        print("✅ Dashboard elements now contain complete worksheet data")
        print("✅ Chart types, fields, and encodings are all accessible")
        print("✅ Clean architecture maintained with proper separation of concerns")
    else:
        print("❌ MigrationEngine Phase 3 integration test FAILED")
        print("❌ Integration layer needs fixes")

    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
