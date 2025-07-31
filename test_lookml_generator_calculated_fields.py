"""Test LookML generator with calculated fields (Book5_calc.twb)."""

import tempfile
import shutil
from pathlib import Path

from tableau_to_looker_parser.generators.lookml_generator import LookMLGenerator
from tableau_to_looker_parser.core.migration_engine import MigrationEngine


def test_lookml_generator_calculated_fields():
    """Test: Book5_calc.twb -> JSON -> LookML generator -> LookML files with calculated fields."""
    # Generate JSON from Book5_calc.twb
    book_name = "Book5_calc.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    print(f"\n=== Testing {book_name} with Calculated Fields ===")
    print(f"Found {len(data.get('calculated_fields', []))} calculated fields:")
    for calc_field in data.get("calculated_fields", []):
        original_formula = calc_field.get("calculation", {}).get("original_formula", "")
        print(f"  - {calc_field['name']}: {original_formula}")

    generator = LookMLGenerator()
    output_dir = Path("sample_twb_files/generated_lookml_book5_calc")
    output_dir.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate LookML files
        generated_files = generator.generate_project_files(data, temp_dir)

        # Validate files generated
        assert "views" in generated_files
        assert "model" in generated_files

        # Calculate expected number of views (tables + self-join aliases)
        expected_views = len(data["tables"])
        alias_views = set()
        for relationship in data.get("relationships", []):
            table_aliases = relationship.get("table_aliases", {})
            for alias, table_ref in table_aliases.items():
                # Check if this is a self-join alias (different from actual table name)
                for table in data["tables"]:
                    if table["table"] == table_ref and alias != table["name"]:
                        alias_views.add(alias)
        expected_views += len(alias_views)

        assert len(generated_files["views"]) == expected_views

        # Validate model content
        with open(generated_files["model"], "r") as f:
            model_content = f.read()
            assert "explore:" in model_content

        # Validate view content and check for calculated fields
        calculated_fields_found = []
        for view_file in generated_files["views"]:
            with open(view_file, "r") as f:
                content = f.read()
                assert "view:" in content

                # Check if this view has calculated fields
                if "# Calculated Fields" in content:
                    print(f"\nCalculated fields found in {Path(view_file).name}:")

                    # Count calculated fields in this view
                    lines = content.split("\n")
                    calc_field_count = 0
                    for line in lines:
                        if line.strip().startswith(
                            "dimension: calculation_"
                        ) or line.strip().startswith("measure: calculation_"):
                            calc_field_count += 1
                            field_name = line.strip().split(":")[1].strip().rstrip(" {")
                            calculated_fields_found.append(field_name)
                            print(f"  - {field_name}")

                    # Look for original formula comments
                    for line in lines:
                        if "# Original Tableau formula:" in line:
                            formula = line.split("# Original Tableau formula:")[
                                1
                            ].strip()
                            print(f"    Formula: {formula}")

        # Copy files for inspection
        shutil.copy2(generated_files["model"], output_dir / "model.lkml")
        for view_file in generated_files["views"]:
            view_name = Path(view_file).stem
            shutil.copy2(view_file, output_dir / f"{view_name}.lkml")

        print(
            f"\n✅ {book_name} -> LookML generation with calculated fields test passed!"
        )
        print(f"Generated files saved to: {output_dir}")
        print("- model.lkml")
        print(f"- {len(generated_files['views'])} view files")
        print(
            f"- Found {len(calculated_fields_found)} calculated fields in views: {calculated_fields_found}"
        )
        print(
            f"Used {len(data['dimensions'])} dimensions, {len(data['measures'])} measures, "
            f"{len(data.get('calculated_fields', []))} calculated fields, {len(data['relationships'])} relationships"
        )

        # Verify we found calculated fields
        assert len(calculated_fields_found) > 0, (
            "No calculated fields were found in generated views"
        )


if __name__ == "__main__":
    test_lookml_generator_calculated_fields()
