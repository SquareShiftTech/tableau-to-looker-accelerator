#!/usr/bin/env python3
"""
Simple test runner for Tableau formula conversion.
Run this to quickly test individual formulas without pytest.
"""

from src.tableau_to_looker_parser.converters.formula_parser import FormulaParser
from src.tableau_to_looker_parser.converters.ast_to_lookml_converter import (
    ASTToLookMLConverter,
)


def test_formula_conversion(tableau_formula: str) -> str:
    """Test a single Tableau formula conversion."""
    try:
        # Initialize parsers
        formula_parser = FormulaParser()
        ast_converter = ASTToLookMLConverter()

        print(f"Input Tableau Formula: {tableau_formula}")

        # Parse Tableau formula to AST
        parse_result = formula_parser.parse_formula(tableau_formula)
        if not parse_result.success:
            result = f"PARSE_ERROR: {parse_result.error_message}"
            print(f"Parse Result: FAILED - {parse_result.error_message}")
            return result

        print("Parse Result: SUCCESS")

        # Get AST from calculated field
        if (
            not parse_result.calculated_field
            or not parse_result.calculated_field.ast_root
        ):
            result = "PARSE_ERROR: No AST generated"
            print("Parse Result: No AST generated")
            return result

        ast = parse_result.calculated_field.ast_root

        # Convert AST to LookML
        lookml_sql = ast_converter.convert_to_lookml(ast, "TABLE")
        print(f"LookML Output: {lookml_sql}")

        return lookml_sql

    except Exception as e:
        error_msg = f"CONVERSION_ERROR: {str(e)}"
        print(f"Error: {error_msg}")
        return error_msg


def run_test_suite():
    """Run a comprehensive test suite."""
    print("=" * 80)
    print("TABLEAU FORMULA TO LOOKML CONVERSION TEST SUITE")
    print("=" * 80)

    test_cases = [
        # Basic field references
        "[Sales]",
        "[Order Date]",
        # Aggregation functions
        "SUM([Sales])",
        "COUNT([Orders])",
        "AVG([Profit])",
        "COUNTD([Customer ID])",
        "VAR([Sales])",
        "STDEV([Profit])",
        # String functions
        "UPPER([Name])",
        "LEN([Title])",
        "LEFT([Code], 3)",
        "CONTAINS([Name], 'John')",
        "ASCII([Char])",
        "PROPER([Name])",
        # Math functions
        "ABS([Profit])",
        "ROUND([Sales], 2)",
        "POWER([Base], 2)",
        # Date functions
        "YEAR([Order Date])",
        "DATEADD([Date], 30, 'day')",
        "DATEDIFF([Start Date], [End Date], 'day')",
        # Type conversion
        "FLOAT([Text Value])",
        "STR([Number])",
        # Logical functions
        "IFNULL([Value], 0)",
        "ISNULL([Field])",
        # Arithmetic operations
        "[Sales] + [Profit]",
        "[Price] * [Quantity]",
        "[Base] ^ 2",
        # Comparisons
        "[Sales] > 1000",
        "[Status] = 'Active'",
        # Conditionals
        "IF [Sales] > 1000 THEN 'High' ELSE 'Low' END",
        # Complex formulas
        "UPPER(LEFT([Name], 5))",
        "SUM([Sales] * [Quantity])",
        "ROUND([Sales] / [Quantity], 2)",
    ]

    results = []
    for i, formula in enumerate(test_cases, 1):
        print(f"\n--- Test {i:2d}: {formula} ---")
        result = test_formula_conversion(formula)
        results.append((formula, result))
        print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    successful = 0
    failed = 0

    for formula, result in results:
        status = (
            "✅ SUCCESS"
            if not result.startswith(("PARSE_ERROR", "CONVERSION_ERROR"))
            else "❌ FAILED"
        )
        if "SUCCESS" in status:
            successful += 1
        else:
            failed += 1
        print(f"{status}: {formula}")
        if "FAILED" in status:
            print(f"         Error: {result}")

    print(f"\nTotal: {len(results)} tests")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success Rate: {successful / len(results) * 100:.1f}%")


def interactive_mode():
    """Interactive mode for testing individual formulas."""
    print("=" * 60)
    print("INTERACTIVE TABLEAU FORMULA TESTING")
    print("=" * 60)
    print("Enter Tableau formulas to test conversion to LookML.")
    print("Type 'quit' or 'exit' to stop.")
    print("Type 'suite' to run the full test suite.")
    print()

    while True:
        try:
            formula = input("Tableau Formula: ").strip()

            if formula.lower() in ["quit", "exit", "q"]:
                print("Goodbye!")
                break
            elif formula.lower() == "suite":
                run_test_suite()
                continue
            elif not formula:
                continue

            print("-" * 40)
            test_formula_conversion(formula)
            print("-" * 40)
            print()

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "suite":
            run_test_suite()
        elif sys.argv[1] == "interactive":
            interactive_mode()
        else:
            # Test specific formula passed as argument
            formula = " ".join(sys.argv[1:])
            test_formula_conversion(formula)
    else:
        # Default: run interactive mode
        interactive_mode()
