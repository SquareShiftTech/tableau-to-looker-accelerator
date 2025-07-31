#!/usr/bin/env python3
"""
Test Error Handling Implementation
Test the complete error handling pipeline with broken formulas.
"""

from src.tableau_to_looker_parser.converters.formula_parser import FormulaParser
from src.tableau_to_looker_parser.converters.ast_to_lookml_converter import (
    ASTToLookMLConverter,
)


def test_formula_parser_error_handling():
    """Test formula parser error handling with broken formulas."""
    parser = FormulaParser()

    # Test cases with broken formulas that should gracefully fail
    broken_formulas = [
        # Syntax errors
        "{BROKEN_SYNTAX [Field : INVALID}",
        "IF [Sales] THEN 'High' /* Missing ELSE/END */",
        "CASE [Category] WHEN /* Incomplete CASE */",
        # Invalid functions
        "UNKNOWN_FUNCTION([Sales])",
        "SUM([Sales], [Profit], [Extra_Invalid_Args])",
        # Tokenization errors
        "[Unclosed_Field_Reference",
        '"Unclosed string literal',
        # Complex nested errors
        "IF (((([Sales] > 1000 AND [Profit] > INVALID_SYNTAX))) THEN 'High' END",
        # LOD syntax errors
        "{INVALID_LOD_TYPE [Region] : SUM([Sales])}",
        "{FIXED [Region : SUM([Sales])}",  # Missing closing bracket
    ]

    print("Testing Formula Parser Error Handling:")
    print("=" * 50)

    for i, formula in enumerate(broken_formulas, 1):
        print(f"\nTest {i}: {formula}")

        try:
            result = parser.parse_formula(formula, field_name=f"broken_field_{i}")

            print(f"  Success: {result.success}")
            print(f"  Error: {result.error_message}")

            # Even failed parses should return a calculated field for fallback
            if result.calculated_field:
                ast = result.calculated_field.ast_root
                print(f"  Fallback AST: {ast.node_type}")
                print(f"  Fallback Value: {ast.value}")

                # Check if migration metadata is present
                if ast.properties:
                    print(
                        f"  Original Formula: {ast.properties.get('original_formula', 'N/A')}"
                    )
                    print(
                        f"  Migration Status: {ast.properties.get('migration_status', 'N/A')}"
                    )
                else:
                    print("  [WARNING] No migration metadata found")
            else:
                print("  [ERROR] No fallback calculated field created")

        except Exception as e:
            print(f"  [CRITICAL ERROR] Unhandled exception: {e}")

    print("\n" + "=" * 50)
    print("Formula Parser Error Handling Test Complete!")


def test_lookml_generation_error_handling():
    """Test LookML generation with fallback calculated fields."""
    parser = FormulaParser()
    converter = ASTToLookMLConverter()

    # Test formulas that should create fallback fields
    test_cases = [
        "UNSUPPORTED_FUNCTION([Sales])",
        "{BROKEN_LOD [Region : INVALID}",
        "IF [Sales] THEN /* Incomplete */",
    ]

    print("\nTesting LookML Generation Error Handling:")
    print("=" * 50)

    for i, formula in enumerate(test_cases, 1):
        print(f"\nTest {i}: {formula}")

        # Step 1: Parse (should create fallback)
        result = parser.parse_formula(formula, field_name=f"error_field_{i}")

        if result.calculated_field:
            # Step 2: Convert to LookML
            ast = result.calculated_field.ast_root
            lookml_sql = converter.convert_to_lookml(ast)

            print(f"  LookML SQL: {lookml_sql}")

            # Check if it's a safe fallback
            if "MIGRATION_REQUIRED" in lookml_sql:
                print("  [OK] Safe fallback generated")
            else:
                print("  [WARNING] May not be safe fallback")
        else:
            print("  [ERROR] No calculated field to convert")

    print("\n" + "=" * 50)
    print("LookML Generation Error Handling Test Complete!")


def test_end_to_end_error_handling():
    """Test complete pipeline with realistic broken calculated fields."""
    from src.tableau_to_looker_parser.generators.view_generator import ViewGenerator

    # Simulate calculated field data with broken formulas
    broken_calc_fields = [
        {
            "name": "broken_case_statement",
            "original_name": "[Broken Case Statement]",
            "field_type": "dimension",
            "role": "dimension",
            "table_name": "orders",
            "calculation": {
                "original_formula": "CASE [Category] WHEN 'Tech' THEN /* BROKEN */",
                "ast": {},  # Empty AST will cause conversion failure
            },
        },
        {
            "name": "unparseable_lod",
            "original_name": "[Unparseable LOD]",
            "field_type": "measure",
            "role": "measure",
            "table_name": "orders",
            "calculation": {
                "original_formula": "{INVALID_LOD [Region] : SUM([Sales])}",
                "ast": {},  # Empty AST will cause conversion failure
            },
        },
    ]

    print("\nTesting End-to-End Error Handling:")
    print("=" * 50)

    generator = ViewGenerator()

    for calc_field in broken_calc_fields:
        print(f"\nProcessing: {calc_field['name']}")
        print(f"Formula: {calc_field['calculation']['original_formula']}")

        try:
            # This should create a fallback field instead of failing
            converted = generator._convert_calculated_field(calc_field, "orders")

            if converted:
                print("  [OK] Converted successfully")
                print(f"  Name: {converted.get('name')}")
                print(f"  SQL: {converted.get('sql')}")

                if converted.get("migration_error"):
                    print("  [OK] Migration error flag set")
                    print(
                        f"  Comment: {converted.get('migration_comment', 'N/A')[:100]}..."
                    )
                else:
                    print("  [WARNING] No migration error metadata")
            else:
                print("  [ERROR] Failed to convert")

        except Exception as e:
            print(f"  [CRITICAL ERROR] Unhandled exception: {e}")

    print("\n" + "=" * 50)
    print("End-to-End Error Handling Test Complete!")


if __name__ == "__main__":
    test_formula_parser_error_handling()
    test_lookml_generation_error_handling()
    test_end_to_end_error_handling()
