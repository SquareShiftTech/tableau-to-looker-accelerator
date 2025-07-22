#!/usr/bin/env python3
"""
Test script to compare ModelGenerator vs ModelGeneratorV2 outputs.
"""

import tempfile
from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.generators.model_generator import ModelGenerator
from tableau_to_looker_parser.generators.model_generator_v2 import ModelGeneratorV2


def test_generator_compatibility(twb_file: str, test_name: str):
    """Test if V2 generator produces same output as original for working cases."""
    print(f"\n=== Testing {test_name} ===")

    # Parse TWB file
    engine = MigrationEngine()
    data = engine.migrate_file(f"sample_twb_files/{twb_file}", "sample_twb_files")

    # Generate with original model generator
    original_gen = ModelGenerator()
    with tempfile.TemporaryDirectory() as temp_dir1:
        original_model_path = original_gen.generate(data, temp_dir1)
        with open(original_model_path, "r") as f:
            original_content = f.read()

    # Generate with V2 model generator
    v2_gen = ModelGeneratorV2()
    with tempfile.TemporaryDirectory() as temp_dir2:
        v2_model_path = v2_gen.generate(data, temp_dir2)
        with open(v2_model_path, "r") as f:
            v2_content = f.read()

    # Compare outputs
    if original_content.strip() == v2_content.strip():
        print(f"✅ {test_name}: IDENTICAL output")
        return True
    else:
        print(f"❌ {test_name}: DIFFERENT output")
        print("\n--- ORIGINAL ---")
        print(original_content)
        print("\n--- V2 ---")
        print(v2_content)
        print("\n--- END COMPARISON ---")
        return False


def test_failing_case(twb_file: str, test_name: str):
    """Test V2 generator on failing cases to see improvement."""
    print(f"\n=== Testing {test_name} (Expected to be improved) ===")

    # Parse TWB file
    engine = MigrationEngine()
    data = engine.migrate_file(f"sample_twb_files/{twb_file}", "sample_twb_files")

    # Generate with V2 model generator
    v2_gen = ModelGeneratorV2()
    with tempfile.TemporaryDirectory() as temp_dir:
        v2_model_path = v2_gen.generate(data, temp_dir)
        with open(v2_model_path, "r") as f:
            v2_content = f.read()

    print("V2 Output:")
    print(v2_content)

    # Check for expected improvements
    joins = [
        line.strip()
        for line in v2_content.split("\n")
        if line.strip().startswith("join:")
    ]
    print(f"\nJoins found: {joins}")
    return v2_content


if __name__ == "__main__":
    print("Testing ModelGeneratorV2 compatibility and improvements...")

    # Test backward compatibility with working cases
    working_cases = [
        ("Book2.twb", "Book2 - No relationships"),
        ("Book3.twb", "Book3 - Simple logical joins"),
        ("StarSchema.twb", "Star Schema - Multiple joins"),
    ]

    all_compatible = True
    for twb_file, test_name in working_cases:
        try:
            compatible = test_generator_compatibility(twb_file, test_name)
            all_compatible = all_compatible and compatible
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {str(e)}")
            all_compatible = False

    print(f"\n{'=' * 60}")
    if all_compatible:
        print("🎉 ALL WORKING CASES MAINTAIN COMPATIBILITY!")
    else:
        print("⚠️  Some working cases have different outputs")

    # Test improvements on failing cases
    failing_cases = [
        ("ManyToManyBridge.twb", "Many-to-Many Bridge"),
        ("MultiChainJoins.twb", "Multi-Chain Joins"),
    ]

    print(f"\n{'=' * 60}")
    print("Testing improvements on previously failing cases...")

    for twb_file, test_name in failing_cases:
        try:
            test_failing_case(twb_file, test_name)
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {str(e)}")
