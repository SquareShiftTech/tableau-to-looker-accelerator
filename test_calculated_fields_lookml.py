#!/usr/bin/env python3
"""
Test script for calculated fields in LookML generation.
"""

import tempfile
import os
from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.generators.view_generator import ViewGenerator


def test_calculated_fields_lookml():
    """Test calculated fields generation in LookML view files."""
    print("\n=== Testing Calculated Fields in LookML Generation ===")

    # Parse Book5 TWB file with calculated fields
    engine = MigrationEngine()
    migration_data = engine.migrate_file(
        "sample_twb_files/Book5_calc.twb", "sample_twb_files"
    )

    print(f"Found {len(migration_data.get('calculated_fields', []))} calculated fields")
    for calc_field in migration_data.get("calculated_fields", []):
        original_formula = calc_field.get("calculation", {}).get("original_formula", "")
        print(f"- {calc_field['name']}: {original_formula}")

    # Generate LookML view files
    view_gen = ViewGenerator()

    with tempfile.TemporaryDirectory() as temp_dir:
        view_files = view_gen.generate_views(migration_data, temp_dir)

        print(f"\nGenerated {len(view_files)} view files:")
        for view_file in view_files:
            print(f"- {os.path.basename(view_file)}")

            # Read and display view content
            with open(view_file, "r") as f:
                content = f.read()

            # Look for calculated fields section
            if "# Calculated Fields" in content:
                print(
                    f"\n--- Content of {os.path.basename(view_file)} (Calculated Fields section) ---"
                )
                lines = content.split("\n")
                calc_section_started = False
                for line in lines:
                    if "# Calculated Fields" in line:
                        calc_section_started = True
                    if (
                        calc_section_started
                        and "# Measures" in line
                        and "Calculated" not in line
                    ):
                        break  # Stop at regular measures section
                    if calc_section_started:
                        print(line)
            else:
                print(f"No calculated fields found in {os.path.basename(view_file)}")


if __name__ == "__main__":
    test_calculated_fields_lookml()
