"""Tests for LookML generator with Book2.twb."""

import tempfile
import shutil
from pathlib import Path
import pytest

from tableau_to_looker_parser.generators.lookml_generator import LookMLGenerator
from tableau_to_looker_parser.core.migration_engine import MigrationEngine


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_lookml_generator_book2():
    """Test: Book2.twb -> JSON -> LookML generator -> LookML files."""
    # Generate JSON from Book2.twb
    book_name = "Book2.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()
    output_dir = Path("sample_twb_files/generated_lookml_book2")
    output_dir.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate LookML files
        generated_files = generator.generate_project_files(data, temp_dir)

        # Validate files generated
        assert "connection" in generated_files
        assert "views" in generated_files
        assert "model" in generated_files
        assert len(generated_files["views"]) == len(data["tables"])

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

        print("✅ Book2.twb -> LookML generation test passed!")
        print(f"Generated files saved to: {output_dir}")
        print("- connection.lkml")
        print("- model.lkml")
        print(f"- {len(generated_files['views'])} view files")
        print(
            f"Used {len(data['dimensions'])} dimensions, {len(data['measures'])} measures, {len(data['relationships'])} relationships"
        )
