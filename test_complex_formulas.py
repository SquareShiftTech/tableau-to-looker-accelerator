#!/usr/bin/env python3
"""
Test complex formulas combining conditionals and arithmetic operations.
"""

from tableau_to_looker_parser.converters.ast_to_lookml_converter import (
    ASTToLookMLConverter,
)
from tableau_to_looker_parser.models.ast_schema import ASTNode, NodeType, DataType


def create_complex_ast_examples():
    """Create AST examples for complex formulas combining conditionals and arithmetic."""

    # Example 1: IF [budget] > 1000000 THEN [budget] * 0.1 ELSE [budget] * 0.05 END
    # This combines: CONDITIONAL + COMPARISON + ARITHMETIC

    # Build the AST bottom-up:

    # Field references
    budget_ref_1 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")
    budget_ref_2 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")
    budget_ref_3 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")

    # Literals
    million_literal = ASTNode(
        node_type=NodeType.LITERAL, value=1000000, data_type=DataType.INTEGER
    )
    tax_high = ASTNode(node_type=NodeType.LITERAL, value=0.1, data_type=DataType.REAL)
    tax_low = ASTNode(node_type=NodeType.LITERAL, value=0.05, data_type=DataType.REAL)

    # Comparison: [budget] > 1000000
    condition = ASTNode(
        node_type=NodeType.COMPARISON,
        operator=">",
        left=budget_ref_1,
        right=million_literal,
    )

    # Arithmetic: [budget] * 0.1
    then_branch = ASTNode(
        node_type=NodeType.ARITHMETIC, operator="*", left=budget_ref_2, right=tax_high
    )

    # Arithmetic: [budget] * 0.05
    else_branch = ASTNode(
        node_type=NodeType.ARITHMETIC, operator="*", left=budget_ref_3, right=tax_low
    )

    # Complete conditional
    complex_conditional = ASTNode(
        node_type=NodeType.CONDITIONAL,
        condition=condition,
        then_branch=then_branch,
        else_branch=else_branch,
    )

    return {
        "budget_tax_calculation": {
            "description": "IF [budget] > 1000000 THEN [budget] * 0.1 ELSE [budget] * 0.05 END",
            "ast": complex_conditional,
        }
    }


def create_nested_arithmetic_conditional():
    """Create nested arithmetic within conditionals."""

    # Example 2: IF ([revenue] - [budget]) > 0 THEN ([revenue] - [budget]) / [budget] ELSE 0 END
    # This is profit margin calculation with nested arithmetic

    # Field references (need multiple copies)
    revenue_1 = ASTNode(node_type=NodeType.FIELD_REF, field_name="revenue")
    budget_1 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")
    revenue_2 = ASTNode(node_type=NodeType.FIELD_REF, field_name="revenue")
    budget_2 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")
    # revenue_3 = ASTNode(node_type=NodeType.FIELD_REF, field_name="revenue")
    budget_3 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")
    # budget_4 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")

    # Literals
    zero_1 = ASTNode(node_type=NodeType.LITERAL, value=0, data_type=DataType.INTEGER)
    zero_2 = ASTNode(node_type=NodeType.LITERAL, value=0, data_type=DataType.INTEGER)

    # Arithmetic: [revenue] - [budget] (for condition)
    profit_condition = ASTNode(
        node_type=NodeType.ARITHMETIC, operator="-", left=revenue_1, right=budget_1
    )

    # Comparison: ([revenue] - [budget]) > 0
    condition = ASTNode(
        node_type=NodeType.COMPARISON, operator=">", left=profit_condition, right=zero_1
    )

    # Arithmetic: [revenue] - [budget] (for then branch numerator)
    profit_numerator = ASTNode(
        node_type=NodeType.ARITHMETIC, operator="-", left=revenue_2, right=budget_2
    )

    # Arithmetic: ([revenue] - [budget]) / [budget] (profit margin)
    profit_margin = ASTNode(
        node_type=NodeType.ARITHMETIC,
        operator="/",
        left=profit_numerator,
        right=budget_3,
    )

    # Complete conditional
    profit_margin_calc = ASTNode(
        node_type=NodeType.CONDITIONAL,
        condition=condition,
        then_branch=profit_margin,
        else_branch=zero_2,
    )

    return {
        "profit_margin": {
            "description": "IF ([revenue] - [budget]) > 0 THEN ([revenue] - [budget]) / [budget] ELSE 0 END",
            "ast": profit_margin_calc,
        }
    }


def test_complex_formulas():
    """Test the AST converter with complex nested formulas."""
    print("=== Testing Complex Formula Conversion ===\n")

    converter = ASTToLookMLConverter()

    # Test 1: Budget tax calculation
    examples_1 = create_complex_ast_examples()

    for name, example in examples_1.items():
        print(f"📋 **{name.replace('_', ' ').title()}**")
        print(f"Original: {example['description']}")

        # Convert AST to LookML
        result = converter.convert_to_lookml(example["ast"], "TABLE")
        print(f"LookML:   {result}")

        # Explain the conversion process
        print("Process:")
        print("  1. Root node: CONDITIONAL")
        print("  2. Condition: COMPARISON ([budget] > 1000000)")
        print("  3. Then: ARITHMETIC ([budget] * 0.1)")
        print("  4. Else: ARITHMETIC ([budget] * 0.05)")
        print("  5. Result: CASE WHEN ... THEN ... ELSE ... END")
        print()

    # Test 2: Profit margin calculation
    examples_2 = create_nested_arithmetic_conditional()

    for name, example in examples_2.items():
        print(f"📋 **{name.replace('_', ' ').title()}**")
        print(f"Original: {example['description']}")

        # Convert AST to LookML
        result = converter.convert_to_lookml(example["ast"], "TABLE")
        print(f"LookML:   {result}")

        # Explain the conversion process
        print("Process:")
        print("  1. Root node: CONDITIONAL")
        print("  2. Condition: COMPARISON (profit > 0)")
        print("     - Where profit = ARITHMETIC ([revenue] - [budget])")
        print("  3. Then: ARITHMETIC (profit / [budget])")
        print("     - Where profit = ARITHMETIC ([revenue] - [budget])")
        print("  4. Else: LITERAL (0)")
        print("  5. Result: Nested CASE with arithmetic expressions")
        print()


def explain_recursion_process():
    """Explain how recursion works in the converter."""
    print("🔄 **How Recursion Works in Complex Formulas:**")
    print()
    print("The AST converter uses **recursive tree traversal**:")
    print()
    print("1. **Entry Point**: `convert_to_lookml(root_node)`")
    print("2. **Dispatch**: Root node type determines conversion method")
    print("3. **Recursive Calls**: Each method calls `_convert_node()` on child nodes")
    print("4. **Build Up**: Results are combined using SQL syntax")
    print()
    print(
        "**Example Tree Walk for**: IF [budget] > 1000 THEN [budget] * 0.1 ELSE 0 END"
    )
    print()
    print("```")
    print("CONDITIONAL")
    print("├── condition: COMPARISON")
    print("│   ├── left: FIELD_REF([budget])     → ${TABLE}.budget")
    print("│   ├── operator: >")
    print("│   └── right: LITERAL(1000)          → 1000")
    print("├── then_branch: ARITHMETIC")
    print("│   ├── left: FIELD_REF([budget])     → ${TABLE}.budget")
    print("│   ├── operator: *")
    print("│   └── right: LITERAL(0.1)           → 0.1")
    print("└── else_branch: LITERAL(0)           → 0")
    print("```")
    print()
    print("**Conversion Steps:**")
    print("1. `_convert_conditional()` called")
    print("2. Recursively convert condition → `(${TABLE}.budget > 1000)`")
    print("3. Recursively convert then_branch → `(${TABLE}.budget * 0.1)`")
    print("4. Recursively convert else_branch → `0`")
    print(
        "5. Combine: `CASE WHEN (${TABLE}.budget > 1000) THEN (${TABLE}.budget * 0.1) ELSE 0 END`"
    )


if __name__ == "__main__":
    test_complex_formulas()
    print("\n" + "=" * 60 + "\n")
    explain_recursion_process()
