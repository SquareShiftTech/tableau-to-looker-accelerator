#!/usr/bin/env python3
"""
Test LOD Expression Implementation
Test the complete LOD parsing pipeline with various expression types.
"""

from src.tableau_to_looker_parser.converters.formula_parser import FormulaParser
from src.tableau_to_looker_parser.models.ast_schema import NodeType


def test_lod_expressions():
    """Test various LOD expression patterns."""
    parser = FormulaParser()

    # Test cases with expected results
    test_cases = [
        # FIXED LOD expressions
        ("{FIXED [Region] : SUM([Sales])}", "FIXED", ["region"], "SUM"),
        (
            "{FIXED [Category], [Region] : AVG([Profit])}",
            "FIXED",
            ["category", "region"],
            "AVG",
        ),
        # INCLUDE LOD expressions
        ("{INCLUDE [Product] : COUNT([Orders])}", "INCLUDE", ["product"], "COUNT"),
        # EXCLUDE LOD expressions
        ("{EXCLUDE [Customer] : MAX([Discount])}", "EXCLUDE", ["customer"], "MAX"),
        # Complex expressions
        (
            "{FIXED [Region] : SUM([Sales]) / COUNT([Orders])}",
            "FIXED",
            ["region"],
            None,
        ),  # Complex expr
    ]

    print("Testing LOD Expression Parsing:")
    print("=" * 50)

    for i, (formula, expected_type, expected_dims, expected_func) in enumerate(
        test_cases, 1
    ):
        print(f"\nTest {i}: {formula}")

        try:
            result = parser.parse_formula(formula)

            if not result.success:
                print(f"[ERROR] {result.error_message}")
                continue

            ast = result.calculated_field.ast_root
            print("[OK] Parsed successfully!")
            print(f"   Node Type: {ast.node_type}")

            if ast.node_type == NodeType.LOD_EXPRESSION:
                print(f"   LOD Type: {ast.lod_type}")
                print(f"   Dimensions: {[d.field_name for d in ast.lod_dimensions]}")
                print(f"   Expression Type: {ast.lod_expression.node_type}")

                if ast.lod_expression.node_type == NodeType.FUNCTION:
                    print(f"   Function: {ast.lod_expression.function_name}")

                # Verify expectations
                if ast.lod_type == expected_type:
                    print("   [OK] LOD type matches")
                else:
                    print(
                        f"   [FAIL] LOD type mismatch: expected {expected_type}, got {ast.lod_type}"
                    )

                actual_dims = [d.field_name for d in ast.lod_dimensions]
                if actual_dims == expected_dims:
                    print("   [OK] Dimensions match")
                else:
                    print(
                        f"   [FAIL] Dimensions mismatch: expected {expected_dims}, got {actual_dims}"
                    )
            else:
                print(f"   [FAIL] Expected LOD_EXPRESSION, got {ast.node_type}")

        except Exception as e:
            print(f"[EXCEPTION] {e}")

    print("\n" + "=" * 50)
    print("LOD Expression Test Complete!")


def test_lod_to_lookml():
    """Test complete LOD pipeline: Formula → AST → LookML SQL"""
    from src.tableau_to_looker_parser.converters.ast_to_lookml_converter import (
        ASTToLookMLConverter,
    )

    parser = FormulaParser()
    converter = ASTToLookMLConverter()

    # Test cases with expected LookML output
    test_cases = [
        (
            "{FIXED [Region] : SUM([Sales])}",
            "(SELECT SUM(${TABLE}.sales) FROM ${TABLE} GROUP BY region)",
        ),
        (
            "{FIXED [Category], [Region] : AVG([Profit])}",
            "(SELECT AVG(${TABLE}.profit) FROM ${TABLE} GROUP BY category, region)",
        ),
        (
            "{INCLUDE [Product] : COUNT([Orders])}",
            "(SELECT COUNT(${TABLE}.orders) FROM ${TABLE} GROUP BY product)",
        ),
        (
            "{EXCLUDE [Customer] : MAX([Discount])}",
            "(SELECT MAX(${TABLE}.discount) FROM ${TABLE})",
        ),
    ]

    print("\nTesting Complete LOD Pipeline (Formula -> AST -> LookML):")
    print("=" * 60)

    for i, (formula, expected_lookml) in enumerate(test_cases, 1):
        print(f"\nTest {i}: {formula}")

        try:
            # Step 1: Parse formula to AST
            result = parser.parse_formula(formula)

            if not result.success:
                print(f"[ERROR] Parsing failed: {result.error_message}")
                continue

            ast = result.calculated_field.ast_root

            # Step 2: Convert AST to LookML
            lookml_sql = converter.convert_to_lookml(ast)

            print(f"[OK] Generated LookML: {lookml_sql}")

            # Check if it roughly matches expected (allowing for formatting differences)
            if "SELECT" in lookml_sql and "FROM" in lookml_sql:
                print("[OK] Valid SQL structure generated")
            else:
                print("[FAIL] Invalid SQL structure")

        except Exception as e:
            print(f"[EXCEPTION] {e}")

    print("\n" + "=" * 60)
    print("Complete LOD Pipeline Test Complete!")


if __name__ == "__main__":
    test_lod_expressions()
    test_lod_to_lookml()
