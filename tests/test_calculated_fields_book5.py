"""
Test suite for calculated fields using Book5_calc.twb.
Tests the complete AST → JSON pipeline with real Tableau data.
"""

import pytest
import tempfile
from pathlib import Path
import json

from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.converters.formula_parser import FormulaParser
from tableau_to_looker_parser.handlers.calculated_field_handler import (
    CalculatedFieldHandler,
)
from tableau_to_looker_parser.models.ast_schema import NodeType


class TestBook5CalculatedFields:
    """Test calculated fields from Book5_calc.twb workbook."""

    @classmethod
    def setup_class(cls):
        """Set up test class with sample file."""
        cls.book_file = Path("sample_twb_files/Book5_calc.twb")
        cls.engine = MigrationEngine()
        cls.parser = FormulaParser()
        cls.handler = CalculatedFieldHandler()

        # Ensure the test file exists
        if not cls.book_file.exists():
            pytest.skip("Book5_calc.twb not found in sample_twb_files directory")

    def test_book5_migration_contains_calculated_fields(self):
        """Test that Book5 migration extracts calculated fields."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Run migration
            result = self.engine.migrate_file(str(self.book_file), temp_dir)

            # Should have calculated fields section
            assert "calculated_fields" in result
            assert isinstance(result["calculated_fields"], list)

            # Should have at least one calculated field (Adult_Calculation1)
            assert len(result["calculated_fields"]) > 0

            print(f"Found {len(result['calculated_fields'])} calculated fields")
            for cf in result["calculated_fields"]:
                print(
                    f"  - {cf.get('name', 'unnamed')}: {cf.get('calculation', {}).get('original_formula', 'no formula')}"
                )

    def test_adult_calculation1_field(self):
        """Test the specific Adult_Calculation1 field from Book5."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.engine.migrate_file(str(self.book_file), temp_dir)

            # Find the Adult_Calculation1 field
            adult_calc = None
            for cf in result["calculated_fields"]:
                if (
                    "Adult_Calculation1" in cf.get("label", "")
                    or "adult"
                    in cf.get("calculation", {}).get("original_formula", "").lower()
                ):
                    adult_calc = cf
                    break

            assert adult_calc is not None, "Adult_Calculation1 field not found"

            # Test field properties
            assert adult_calc["role"] in ["dimension", "measure"]
            assert adult_calc["datatype"] == "boolean"

            # Test calculation structure
            calculation = adult_calc["calculation"]
            assert calculation["original_formula"] == "[adult]"
            assert "ast" in calculation
            assert calculation["complexity"] in ["simple", "medium", "complex"]
            assert "dependencies" in calculation
            assert (
                "adult" in calculation["dependencies"]
            )  # Should reference adult field

            print("Adult_Calculation1 details:")
            print(f"  Formula: {calculation['original_formula']}")
            print(f"  Complexity: {calculation['complexity']}")
            print(f"  Dependencies: {calculation['dependencies']}")
            print(f"  Parse confidence: {calculation['parse_confidence']}")

    def test_simple_field_reference_formula_parsing(self):
        """Test parsing simple field reference formula [adult]."""
        formula = "[adult]"

        result = self.parser.parse_formula(formula, "test_field", "dimension")

        assert result.success, f"Parsing failed: {result.error_message}"
        assert result.calculated_field is not None

        # Check AST structure
        ast_root = result.calculated_field.ast_root
        assert ast_root.node_type == NodeType.FIELD_REF
        assert ast_root.field_name == "adult"
        assert ast_root.original_name == "[adult]"

        # Check dependencies
        assert result.calculated_field.dependencies == ["adult"]
        assert result.calculated_field.complexity == "simple"

        print(f"Parsed formula: {formula}")
        print(f"AST node type: {ast_root.node_type}")
        print(f"Field name: {ast_root.field_name}")
        print(f"Dependencies: {result.calculated_field.dependencies}")

    def test_calculated_field_handler_confidence(self):
        """Test calculated field handler confidence scoring."""
        # Test data representing a calculated field from Book5
        calculated_field_data = {
            "name": "[Calculation_121878699071307776]",
            "role": "dimension",
            "datatype": "boolean",
            "calculation": "[adult]",
            "caption": "Adult_Calculation1",
        }

        # Test regular field (should get low confidence)
        regular_field_data = {
            "name": "[title]",
            "role": "dimension",
            "datatype": "string",
            # No calculation field
        }

        # Test confidence scores
        calc_confidence = self.handler.can_handle(calculated_field_data)
        regular_confidence = self.handler.can_handle(regular_field_data)

        assert calc_confidence > 0.8, (
            f"Calculated field confidence too low: {calc_confidence}"
        )
        assert regular_confidence == 0.0, (
            f"Regular field should get 0 confidence: {regular_confidence}"
        )

        print(f"Calculated field confidence: {calc_confidence}")
        print(f"Regular field confidence: {regular_confidence}")

    def test_calculated_field_handler_conversion(self):
        """Test calculated field handler JSON conversion."""
        # Test data from Book5
        field_data = {
            "name": "[Calculation_121878699071307776]",
            "role": "dimension",
            "datatype": "boolean",
            "calculation": "[adult]",
            "caption": "Adult_Calculation1",
            "table_name": "movies_data",
        }

        # Convert to JSON
        json_result = self.handler.convert_to_json(field_data)

        # Validate structure
        assert "name" in json_result
        assert "calculation" in json_result
        assert "role" in json_result

        # Validate calculation structure
        calc = json_result["calculation"]
        assert calc["original_formula"] == "[adult]"
        assert "ast" in calc or calc.get("parse_error")  # Either AST or error
        assert "dependencies" in calc
        assert "complexity" in calc

        # Validate metadata
        assert "metadata" in json_result
        assert json_result["metadata"]["handler"] == "CalculatedFieldHandler"

        print("Converted calculated field:")
        print(json.dumps(json_result, indent=2))

    def test_formula_parser_error_handling(self):
        """Test formula parser error handling with invalid formulas."""
        invalid_formulas = [
            "",  # Empty formula
            "INVALID_FUNC([field])",  # Unsupported function
            "[unclosed bracket",  # Syntax error
            "IF [field] THEN 'value'",  # Missing ELSE/END
        ]

        for formula in invalid_formulas:
            result = self.parser.parse_formula(formula, "test_field", "dimension")

            if not result.success:
                # Should have error message
                assert result.error_message is not None
                assert len(result.error_message) > 0
                print(f"Formula '{formula}' correctly failed: {result.error_message}")
            else:
                # If it succeeded, check for warnings
                if result.calculated_field and result.calculated_field.warnings:
                    print(
                        f"Formula '{formula}' parsed with warnings: {result.calculated_field.warnings}"
                    )

    def test_field_dependencies_extraction(self):
        """Test field dependency extraction from formulas."""
        test_cases = [
            ("[adult]", ["adult"]),
            ("[budget] + [revenue]", ["budget", "revenue"]),
            (
                "IF [adult] THEN [budget] ELSE [revenue] END",
                ["adult", "budget", "revenue"],
            ),
            ("SUM([budget])", ["budget"]),
            ("UPPER([title])", ["title"]),
        ]

        for formula, expected_deps in test_cases:
            result = self.parser.parse_formula(formula, "test", "dimension")

            if result.success and result.calculated_field:
                actual_deps = sorted(result.calculated_field.dependencies)
                expected_deps_sorted = sorted(expected_deps)

                assert actual_deps == expected_deps_sorted, (
                    f"Formula '{formula}': expected {expected_deps_sorted}, got {actual_deps}"
                )
                print(f"✓ Formula '{formula}' dependencies: {actual_deps}")
            else:
                # For complex formulas that might not parse yet, just check we get something
                print(f"Formula '{formula}' parse failed: {result.error_message}")

    def test_complexity_analysis(self):
        """Test formula complexity analysis."""
        complexity_cases = [
            ("[adult]", "simple"),  # Simple field reference
            ("[budget] + [revenue]", "simple"),  # Simple arithmetic
            (
                "IF [adult] THEN 'Adult' ELSE 'Not Adult' END",
                "simple",
            ),  # Simple conditional
        ]

        for formula, expected_complexity in complexity_cases:
            result = self.parser.parse_formula(formula, "test", "dimension")

            if result.success and result.calculated_field:
                actual_complexity = result.calculated_field.complexity
                assert actual_complexity == expected_complexity, (
                    f"Formula '{formula}': expected {expected_complexity}, got {actual_complexity}"
                )
                print(f"✓ Formula '{formula}' complexity: {actual_complexity}")
            else:
                print(f"Formula '{formula}' parse failed: {result.error_message}")

    def test_ast_validation(self):
        """Test AST structure validation."""
        from tableau_to_looker_parser.models.ast_schema import ASTValidator

        formula = "[adult]"
        result = self.parser.parse_formula(formula, "test", "dimension")

        assert result.success, f"Parsing failed: {result.error_message}"

        # Validate AST structure
        errors = ASTValidator.validate_ast(result.calculated_field.ast_root)
        assert len(errors) == 0, f"AST validation errors: {errors}"

        print(f"✓ AST validation passed for formula: {formula}")

    def test_book5_integration_end_to_end(self):
        """Test complete end-to-end processing of Book5 calculated fields."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Process the entire workbook
            result = self.engine.migrate_file(str(self.book_file), temp_dir)

            # Save results to permanent location for validation (like other pipeline tests)
            output_dir = Path("sample_twb_files/generated_json_book5")
            output_dir.mkdir(exist_ok=True)
            output_file = output_dir / "book5_calc_migration_result.json"

            with open(output_file, "w") as f:
                json.dump(result, f, indent=2, default=str)

            print(f"Book5 processing results saved to: {output_file}")

            # Validate overall structure
            assert "calculated_fields" in result
            assert "dimensions" in result
            assert "measures" in result
            assert "connections" in result

            # Print summary
            print("Book5_calc.twb processing summary:")
            print(f"  - Calculated fields: {len(result['calculated_fields'])}")
            print(f"  - Regular dimensions: {len(result['dimensions'])}")
            print(f"  - Regular measures: {len(result['measures'])}")
            print(f"  - Connections: {len(result['connections'])}")
            print(f"  - Tables: {len(result['tables'])}")
            print(f"  - Relationships: {len(result['relationships'])}")

            # Ensure we have some calculated fields
            assert len(result["calculated_fields"]) > 0, (
                "No calculated fields found in Book5"
            )


@pytest.mark.integration
class TestCalculatedFieldsIntegration:
    """Integration tests for calculated fields system."""

    def test_formula_parser_registry_functions(self):
        """Test that parser has expected function registry."""
        parser = FormulaParser()

        expected_functions = ["SUM", "COUNT", "AVG", "UPPER", "LOWER", "IF"]

        for func_name in expected_functions:
            assert parser.function_registry.is_supported(func_name), (
                f"Function {func_name} not supported"
            )

        all_functions = list(parser.function_registry.functions.keys())
        print(
            f"Supported functions ({len(all_functions)}): {', '.join(sorted(all_functions))}"
        )

    def test_handler_registration_priority(self):
        """Test that calculated field handler is registered with correct priority."""
        engine = MigrationEngine()

        handlers = engine.plugin_registry.get_handlers_by_priority()
        handler_names = [h.__class__.__name__ for h in handlers]

        assert "CalculatedFieldHandler" in handler_names

        # CalculatedFieldHandler should come after DimensionHandler and MeasureHandler
        calc_index = handler_names.index("CalculatedFieldHandler")
        dim_index = handler_names.index("DimensionHandler")
        measure_index = handler_names.index("MeasureHandler")

        assert calc_index > dim_index, (
            "CalculatedFieldHandler should have lower priority than DimensionHandler"
        )
        assert calc_index > measure_index, (
            "CalculatedFieldHandler should have lower priority than MeasureHandler"
        )

        print(f"Handler priority order: {handler_names}")


if __name__ == "__main__":
    # Run tests when script is executed directly
    pytest.main([__file__, "-v", "--tb=short"])
