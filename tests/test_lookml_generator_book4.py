"""Tests for LookML generator with Book4.twb."""

import tempfile
import shutil
from pathlib import Path
import pytest

from tableau_to_looker_parser.generators.lookml_generator import LookMLGenerator
from tableau_to_looker_parser.core.migration_engine import MigrationEngine


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_lookml_generator_book4():
    """Test: Book4.twb -> JSON -> LookML generator -> LookML files."""
    # Generate JSON from Book4.twb
    book_name = "Book4.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()
    output_dir = Path("sample_twb_files/generated_lookml_book4")
    output_dir.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate LookML files
        generated_files = generator.generate_project_files(data, temp_dir)

        # Validate files generated
        assert "connection" in generated_files
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

        # Validate content
        with open(generated_files["connection"], "r") as f:
            assert "connection:" in f.read()

        with open(generated_files["model"], "r") as f:
            model_content = f.read()
            assert "explore:" in model_content

        for view_file in generated_files["views"]:
            with open(view_file, "r") as f:
                content = f.read()
                assert "view:" in content
                assert "dimension:" in content
                assert "measure:" in content

        # Copy files for inspection
        shutil.copy2(generated_files["connection"], output_dir / "connection.lkml")
        shutil.copy2(generated_files["model"], output_dir / "model.lkml")
        for view_file in generated_files["views"]:
            view_name = Path(view_file).stem
            shutil.copy2(view_file, output_dir / f"{view_name}.lkml")

        print("✅ Book4.twb -> LookML generation test passed!")
        print(f"Generated files saved to: {output_dir}")
        print("- connection.lkml")
        print("- model.lkml")
        print(f"- {len(generated_files['views'])} view files")
        print(
            f"Used {len(data['dimensions'])} dimensions, {len(data['measures'])} measures, {len(data['relationships'])} relationships"
        )
