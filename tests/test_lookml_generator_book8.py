"""Tests for LookML generator with book8.twb."""

import tempfile
import shutil
from pathlib import Path
import pytest

from tableau_to_looker_parser.generators.lookml_generator import LookMLGenerator
from tableau_to_looker_parser.core.migration_engine import MigrationEngine


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_lookml_generator_book8():
    """Test: book8.twb -> JSON -> LookML generator -> LookML files."""
    # Generate JSON from book8.twb
    book_name = "book8.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()
    output_dir = Path("sample_twb_files/generated_lookml_book8")
    output_dir.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate LookML files
        generated_files = generator.generate_project_files(data, temp_dir)

        # Validate files generated
        assert "views" in generated_files
        assert "model" in generated_files
        assert len(generated_files["views"]) == len(data["tables"])

        # Validate model content - book8 should have joins
        with open(generated_files["model"], "r") as f:
            model_content = f.read()
            assert "explore:" in model_content
            assert "join:" in model_content  # book8 has physical relationships
            # Specifically check for the Orders -> Returns join
            assert "join: returns" in model_content
            assert "sql_on: ${orders.order_id} = ${returns.order_id}" in model_content

        # Validate that each view has proper structure
        dimensions_found = False
        measures_found = False

        for view_file in generated_files["views"]:
            with open(view_file, "r") as f:
                content = f.read()
                assert "view:" in content

                # Check for dimensions and measures across all views
                if "dimension:" in content:
                    dimensions_found = True
                if "measure:" in content:
                    measures_found = True

        # book8 should have measures (count measures are always generated)
        assert measures_found, "No measures found in any view"

        # book8 should have dimensions (Orders and Returns have multiple fields)
        assert dimensions_found, "No dimensions found in any view"

        # Copy files for inspection
        shutil.copy2(generated_files["model"], output_dir / "model.lkml")
        for view_file in generated_files["views"]:
            view_name = Path(view_file).stem
            shutil.copy2(view_file, output_dir / f"{view_name}.lkml")

        print("✅ book8.twb -> LookML generation test passed!")
        print(f"Generated files saved to: {output_dir}")
        print("- model.lkml (with physical joins)")
        print(f"- {len(generated_files['views'])} view files")
        print(
            f"Used {len(data['dimensions'])} dimensions, {len(data['measures'])} measures, {len(data['relationships'])} relationships"
        )
