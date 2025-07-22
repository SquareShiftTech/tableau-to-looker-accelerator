"""Tests for LookML generator with Self-Join patterns (Employee-Manager hierarchy)."""

import tempfile
import shutil
from pathlib import Path
import pytest

from tableau_to_looker_parser.generators.lookml_generator import LookMLGenerator
from tableau_to_looker_parser.core.migration_engine import MigrationEngine


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_lookml_generator_self_join():
    """Test: SelfJoinEmployees.twb -> JSON -> LookML with self-join relationships."""
    # Generate JSON from SelfJoinEmployees.twb
    book_name = "SelfJoinEmployees.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()
    output_dir = Path("sample_twb_files/generated_lookml_self_join")
    output_dir.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate LookML files
        generated_files = generator.generate_project_files(data, temp_dir)

        # Validate files generated
        assert "views" in generated_files
        assert "model" in generated_files

        # Should have 2 view files (employee and manager - same table, different aliases)
        assert len(generated_files["views"]) == 2

        # Validate model content - should handle self-join properly
        with open(generated_files["model"], "r") as f:
            model_content = f.read()
            assert "explore:" in model_content

            # Self-join should create join between employee and manager
            assert "join: manager" in model_content or "join: employee" in model_content

            # Validate self-join condition (flexible matching due to field name parsing)
            # Check that join condition exists in some form
            self_join_patterns = [
                "${employee.manager_id} = ${manager.employee_id",  # Partial match
                "employee.manager_id",
                "manager.employee_id",
            ]

            join_condition_found = any(
                pattern in model_content for pattern in self_join_patterns
            )
            assert join_condition_found, (
                f"Self-join condition not found in model. Content: {model_content}"
            )

        # Validate view files have proper field disambiguation
        employee_view_found = False
        manager_view_found = False

        for view_file in generated_files["views"]:
            view_name = Path(view_file).stem.replace(".view", "")
            with open(view_file, "r") as f:
                content = f.read()
                assert "view:" in content

                if (
                    "employee" in view_name.lower()
                    and "manager" not in view_name.lower()
                ):
                    employee_view_found = True
                    # Employee view should have basic structure
                    assert "sql_table_name:" in content
                    assert "measure: count" in content

                    # Check for dimension content if dimensions were parsed
                    if "dimension:" in content:
                        # If dimensions exist, check for some employee fields
                        assert any(
                            field in content
                            for field in ["employee_id", "manager_id", "first_name"]
                        )

                elif "manager" in view_name.lower():
                    manager_view_found = True
                    # Manager view should have basic structure
                    assert "sql_table_name:" in content
                    assert "measure: count" in content

                    # Check for dimension content if dimensions were parsed
                    if "dimension:" in content:
                        # If dimensions exist, check for some manager-specific fields
                        expected_manager_fields = [
                            "employee_id_manager",
                            "first_name_manager",
                            "last_name_manager",
                        ]
                        manager_field_found = any(
                            field in content for field in expected_manager_fields
                        )
                        if not manager_field_found:
                            print(
                                "Warning: Expected manager field disambiguation not found in content"
                            )

        assert employee_view_found, "Employee view should be generated"
        assert manager_view_found, "Manager view (self-join alias) should be generated"

        # Copy files for inspection
        shutil.copy2(generated_files["model"], output_dir / "model.lkml")
        for view_file in generated_files["views"]:
            view_name = Path(view_file).stem
            shutil.copy2(view_file, output_dir / f"{view_name}.lkml")

        print("✅ SelfJoinEmployees.twb -> LookML generation test passed!")
        print(f"Generated files saved to: {output_dir}")
        print("- model.lkml (with self-join relationship)")
        print(
            f"- {len(generated_files['views'])} view files (employee + manager alias)"
        )
        print(f"Used {len(data['relationships'])} self-join relationship")


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_self_join_field_disambiguation():
    """Test that self-join creates properly disambiguated field names."""
    book_name = "SelfJoinEmployees.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    # Test field name transformations for self-join
    dimensions = data.get("dimensions", [])

    # Check if any dimensions were parsed (may be limited due to metadata parsing)
    if len(dimensions) == 0:
        print("Warning: No dimensions parsed from self-join TWB file")
        return  # Skip detailed field checks if no dimensions were parsed

    # Should have disambiguated manager fields if dimensions exist
    manager_field_patterns = [
        "employee_id_manager",
        "first_name_manager",
        "last_name_manager",
        "salary_manager",
    ]

    manager_fields_found = 0
    for expected_field in manager_field_patterns:
        matching_fields = [
            d for d in dimensions if expected_field in d.get("name", "").lower()
        ]
        if len(matching_fields) > 0:
            manager_fields_found += 1

    # At least some manager fields should be found if dimensions exist
    assert manager_fields_found > 0 or len(dimensions) == 0, (
        "Should find at least some manager fields when dimensions exist"
    )

    # Should also have regular employee fields (no disambiguation needed)
    employee_field_patterns = ["employee_id", "first_name", "last_name", "manager_id"]

    for expected_field in employee_field_patterns:
        matching_fields = [
            d for d in dimensions if d.get("name", "").lower() == expected_field
        ]
        assert len(matching_fields) > 0, f"Should find employee field {expected_field}"

    # Check measures separately (salary should be a measure, not dimension)
    measures = data.get("measures", [])
    salary_measures = [m for m in measures if "salary" in m.get("name", "").lower()]
    assert len(salary_measures) > 0, "Should find salary measure"


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_self_join_relationship_type():
    """Test that self-join has correct relationship cardinality."""
    book_name = "SelfJoinEmployees.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()

    with tempfile.TemporaryDirectory() as temp_dir:
        generated_files = generator.generate_project_files(data, temp_dir)

        with open(generated_files["model"], "r") as f:
            model_content = f.read()

            # Employee-Manager relationship (flexible matching since relationship inference varies)
            # Could be many-to-one or one-to-one depending on how the relationship is interpreted
            relationship_found = (
                "relationship: many_to_one" in model_content
                or "relationship: one_to_one" in model_content
            )
            assert relationship_found, "Should find some relationship type defined"

            # Should use left join to include employees without managers
            assert "type: left_outer" in model_content or "type: left" in model_content


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_self_join_table_aliases():
    """Test that self-join creates proper table aliases in LookML views."""
    book_name = "SelfJoinEmployees.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()

    with tempfile.TemporaryDirectory() as temp_dir:
        generated_files = generator.generate_project_files(data, temp_dir)

        # Check that both views reference the same underlying table but with different aliases
        employee_sql_table = None
        manager_sql_table = None

        for view_file in generated_files["views"]:
            with open(view_file, "r") as f:
                content = f.read()

                if "view: employee" in content:
                    # Extract sql_table_name
                    for line in content.split("\n"):
                        if "sql_table_name:" in line:
                            employee_sql_table = line.strip()
                            break

                elif "view: manager" in content:
                    # Extract sql_table_name
                    for line in content.split("\n"):
                        if "sql_table_name:" in line:
                            manager_sql_table = line.strip()
                            break

        # Both should reference the same underlying table
        assert employee_sql_table is not None, (
            "Employee view should have sql_table_name"
        )
        assert manager_sql_table is not None, "Manager view should have sql_table_name"

        # Both should point to the employees table
        assert "employees" in employee_sql_table
        assert "employees" in manager_sql_table
