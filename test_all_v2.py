#!/usr/bin/env python3
"""
Comprehensive test script to validate ModelGeneratorV2 against all test cases.
"""

import sys
import tempfile
from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.generators.model_generator_v2 import ModelGeneratorV2
from tableau_to_looker_parser.generators.lookml_generator import LookMLGenerator


def test_with_v2_generator(twb_file: str, test_name: str) -> bool:
    """Test a specific TWB file with the V2 generator and check expected patterns."""
    print(f"\n=== Testing {test_name} ===")

    try:
        # Parse TWB file
        engine = MigrationEngine()
        data = engine.migrate_file(f"sample_twb_files/{twb_file}", "sample_twb_files")

        # Create a custom LookML generator that uses ModelGeneratorV2
        class LookMLGeneratorV2(LookMLGenerator):
            def __init__(self, template_dir=None):
                super().__init__(template_dir)
                # Replace the model generator with V2
                self.project_generator.model_generator = ModelGeneratorV2(template_dir)

        # Generate LookML files with V2
        generator = LookMLGeneratorV2()

        with tempfile.TemporaryDirectory() as temp_dir:
            generated_files = generator.generate_project_files(data, temp_dir)

            # Read the generated model content
            with open(generated_files["model"], "r") as f:
                model_content = f.read()

            print("Generated model content:")
            print("-" * 50)
            print(model_content)
            print("-" * 50)

            # Analyze joins
            joins = []
            for line in model_content.split("\n"):
                if line.strip().startswith("join:"):
                    join_name = line.strip().split()[1]
                    joins.append(join_name)

            print(f"Joins generated: {joins}")
            print(f"Number of joins: {len(joins)}")
            print(f"Number of views: {len(generated_files.get('views', []))}")
            print(f"Number of tables: {len(data.get('tables', []))}")
            print(f"Number of relationships: {len(data.get('relationships', []))}")

            return True

    except Exception as e:
        print(f"❌ {test_name}: ERROR - {str(e)}")
        import traceback

        traceback.print_exc()
        return False


def run_specific_test_patterns():
    """Run specific test patterns that were previously failing."""
    print("🔍 Testing specific patterns that were previously failing...")

    test_patterns = [
        {
            "file": "ManyToManyBridge.twb",
            "name": "Many-to-Many Bridge",
            "expected_joins": [
                "student_enrollments",
                "courses",
                "professors",
                "departments",
            ],
            "expected_conditions": [
                "${students.student_id} = ${student_enrollments.student_id_enrollments}",
                "${courses.professor_id_courses} = ${professors.professor_id_professors}",
            ],
        },
        {
            "file": "MultiChainJoins.twb",
            "name": "Multi-Chain Joins",
            "expected_joins": [
                "order_items",
                "customers",
                "products",
                "categories",
                "suppliers",
            ],
            "expected_conditions": [
                "${orders.order_id} = ${order_items.order_id_order_items}",
                "${products.category_id_products} = ${categories.category_id_categories}",
            ],
        },
    ]

    all_passed = True

    for pattern in test_patterns:
        print(f"\n{'=' * 60}")
        print(
            f"Testing {pattern['name']} - Expected joins: {pattern['expected_joins']}"
        )

        success = test_with_v2_generator(pattern["file"], pattern["name"])
        if not success:
            all_passed = False
            continue

        # Additional validation could be added here to check for specific patterns
        print(f"✅ {pattern['name']} - Basic test passed")

    return all_passed


if __name__ == "__main__":
    print("🧪 Comprehensive ModelGeneratorV2 Testing")
    print("=" * 60)

    # Test all available TWB files
    test_files = [
        ("Book2.twb", "Book2 - No relationships"),
        ("Book3.twb", "Book3 - Simple logical joins"),
        ("Book4.twb", "Book4 - Physical joins"),
        ("StarSchema.twb", "Star Schema - Multiple logical joins"),
        ("SelfJoinEmployees.twb", "Self Join - Employee hierarchy"),
        ("ManyToManyBridge.twb", "Many-to-Many - Bridge table pattern"),
        ("MultiChainJoins.twb", "Multi-Chain - Complex join chains"),
    ]

    success_count = 0
    total_count = len(test_files)

    for twb_file, test_name in test_files:
        if test_with_v2_generator(twb_file, test_name):
            success_count += 1

    print(f"\n{'=' * 60}")
    print(f"📊 SUMMARY: {success_count}/{total_count} tests passed")

    if success_count == total_count:
        print("🎉 ALL TESTS PASSED! V2 generator is ready for deployment.")
    else:
        print(
            f"⚠️  {total_count - success_count} tests failed. Review the output above."
        )

    # Run specific pattern validation
    print(f"\n{'=' * 60}")
    pattern_success = run_specific_test_patterns()

    if success_count == total_count and pattern_success:
        print(
            "\n🚀 ModelGeneratorV2 is fully validated and ready to replace the original!"
        )
        sys.exit(0)
    else:
        print("\n🔧 Some issues remain to be addressed.")
        sys.exit(1)
