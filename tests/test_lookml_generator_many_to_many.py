"""Tests for LookML generator with Many-to-Many Bridge Table patterns."""

import tempfile
import shutil
from pathlib import Path
import pytest

from tableau_to_looker_parser.generators.lookml_generator import LookMLGenerator
from tableau_to_looker_parser.core.migration_engine import MigrationEngine


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_lookml_generator_many_to_many_bridge():
    """Test: ManyToManyBridge.twb -> JSON -> LookML with bridge table relationships."""
    # Generate JSON from ManyToManyBridge.twb
    book_name = "ManyToManyBridge.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()
    output_dir = Path("sample_twb_files/generated_lookml_many_to_many")
    output_dir.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate LookML files
        generated_files = generator.generate_project_files(data, temp_dir)

        # Validate files generated
        assert "views" in generated_files
        assert "model" in generated_files

        # Should have 5 view files (students, student_enrollments, courses, professors, departments)
        assert len(generated_files["views"]) == 5

        # Validate model content - should handle many-to-many via bridge table
        with open(generated_files["model"], "r") as f:
            model_content = f.read()
            assert "explore:" in model_content

            # Many-to-many pattern: Students ↔ Student_Enrollments ↔ Courses
            assert "join: student_enrollments" in model_content
            assert "join: courses" in model_content
            assert "join: professors" in model_content
            assert "join: departments" in model_content

            # Validate proper bridge table field mappings (V2 Generator creates optimal join path)
            # Students -> Student_Enrollments (Bridge table)
            assert (
                "${students.student_id} = ${student_enrollments.student_id_enrollments}"
                in model_content
            )

            # Student_Enrollments -> Courses (Through bridge)
            assert (
                "${student_enrollments.course_id_enrollments} = ${courses.course_id_courses}"
                in model_content
            )

            # Courses -> Professors (Direct relationship)
            assert (
                "${courses.professor_id_courses} = ${professors.professor_id_professors}"
                in model_content
            )

            # Courses -> Departments (V2 chooses optimal path through courses)
            assert (
                "${courses.department_id_courses} = ${departments.department_id_departments}"
                in model_content
            )

        # Validate bridge table view is generated (V2 focuses on model relationships)
        bridge_view_found = False
        for view_file in generated_files["views"]:
            view_name = Path(view_file).stem.replace(".view", "")
            with open(view_file, "r") as f:
                content = f.read()
                assert "view:" in content
                assert "measure: count" in content  # Basic measures should be present

                if "student_enrollments" in view_name:
                    bridge_view_found = True
                    # Bridge table view should exist with basic structure
                    assert "sql_table_name:" in content

        assert bridge_view_found, (
            "Bridge table view (student_enrollments) should be generated"
        )

        # Copy files for inspection
        shutil.copy2(generated_files["model"], output_dir / "model.lkml")
        for view_file in generated_files["views"]:
            view_name = Path(view_file).stem
            shutil.copy2(view_file, output_dir / f"{view_name}.lkml")

        print("✅ ManyToManyBridge.twb -> LookML generation test passed!")
        print(f"Generated files saved to: {output_dir}")
        print("- model.lkml (with many-to-many bridge relationships)")
        print(f"- {len(generated_files['views'])} view files (including bridge table)")
        print(
            f"Used {len(data['relationships'])} relationships in many-to-many pattern"
        )


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_many_to_many_relationship_cardinalities():
    """Test that many-to-many relationships have correct cardinalities."""
    book_name = "ManyToManyBridge.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()

    with tempfile.TemporaryDirectory() as temp_dir:
        generated_files = generator.generate_project_files(data, temp_dir)

        with open(generated_files["model"], "r") as f:
            model_content = f.read()

            # Bridge table relationships should have specific cardinalities:
            # V2 Generator creates 4 optimal joins with many_to_one relationships
            # Students -> Enrollments: many_to_one
            # Enrollments -> Courses: many_to_one
            # Courses -> Professors: many_to_one
            # Courses -> Departments: many_to_one

            assert "relationship: many_to_one" in model_content
            relationship_count = model_content.count("relationship:")
            assert relationship_count >= 4, (
                "Should have at least 4 relationships defined"
            )


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_many_to_many_bridge_table_measures():
    """Test that bridge table generates appropriate measures for many-to-many analysis."""
    book_name = "ManyToManyBridge.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()

    with tempfile.TemporaryDirectory() as temp_dir:
        generated_files = generator.generate_project_files(data, temp_dir)

        # Check bridge table view for enrollment-specific measures
        for view_file in generated_files["views"]:
            view_name = Path(view_file).stem.replace(".view", "")
            if "student_enrollments" in view_name:
                with open(view_file, "r") as f:
                    content = f.read()

                    # Should have measures for analyzing enrollment data
                    assert "measure: count" in content

                    # Should have sum measure for credits_earned
                    measures = data.get("measures", [])
                    credit_measures = [
                        m for m in measures if "credits" in m.get("name", "").lower()
                    ]
                    if credit_measures:
                        assert any(
                            "sum" in m.get("aggregation", "").lower()
                            for m in credit_measures
                        )


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_many_to_many_complex_field_mapping():
    """Test complex field name mapping in many-to-many relationships."""
    book_name = "ManyToManyBridge.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()

    with tempfile.TemporaryDirectory() as temp_dir:
        generated_files = generator.generate_project_files(data, temp_dir)

        with open(generated_files["model"], "r") as f:
            model_content = f.read()

            # Test that V2 generator creates proper field mappings in join conditions
            # Bridge table should have properly disambiguated foreign keys
            bridge_field_patterns = [
                "student_id_enrollments",  # Bridge table foreign key
                "course_id_enrollments",  # Bridge table foreign key
                "course_id_courses",  # Target table primary key
                "professor_id_courses",  # Courses foreign key
                "professor_id_professors",  # Professors primary key
                "department_id_courses",  # Courses foreign key to departments
                "department_id_departments",  # Departments primary key
            ]

            for expected_field in bridge_field_patterns:
                assert expected_field in model_content, (
                    f"Should find field {expected_field} in model join conditions"
                )


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_many_to_many_explore_structure():
    """Test that many-to-many creates a proper explore structure for analysis."""
    book_name = "ManyToManyBridge.twb"
    twb_file = Path(f"sample_twb_files/{book_name}")

    engine = MigrationEngine()
    data = engine.migrate_file(str(twb_file), str(twb_file.parent))

    generator = LookMLGenerator()

    with tempfile.TemporaryDirectory() as temp_dir:
        generated_files = generator.generate_project_files(data, temp_dir)

        with open(generated_files["model"], "r") as f:
            model_content = f.read()

            # Should have a primary explore that enables analysis across the many-to-many relationship
            assert "explore:" in model_content

            # The explore should allow analyzing:
            # - Students and their courses (via enrollments)
            # - Courses and their students (via enrollments)
            # - Department course offerings
            # - Professor teaching loads

            # Should consolidate into a single comprehensive explore
            explores = model_content.count("explore:")
            assert explores >= 1, (
                "Should have at least one explore for many-to-many analysis"
            )
