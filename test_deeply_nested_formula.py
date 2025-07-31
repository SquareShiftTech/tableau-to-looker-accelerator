#!/usr/bin/env python3
"""
Test deeply nested formula with multiple conditional and arithmetic combinations.
"""

from tableau_to_looker_parser.converters.ast_to_lookml_converter import (
    ASTToLookMLConverter,
)
from tableau_to_looker_parser.models.ast_schema import ASTNode, NodeType, DataType


def create_movie_rating_formula():
    """
    Create a complex movie rating formula that combines multiple conditionals and arithmetic:

    IF [budget] > 100000000 THEN
        IF [revenue] > [budget] * 2 THEN "Blockbuster Success"
        ELSE "Expensive Flop"
    ELSE
        IF ([revenue] - [budget]) / [budget] > 0.5 THEN "Indie Success"
        ELSE "Low Budget"
    END
    """

    # Field references (we need many copies)
    budget_refs = [
        ASTNode(node_type=NodeType.FIELD_REF, field_name="budget") for _ in range(6)
    ]
    revenue_refs = [
        ASTNode(node_type=NodeType.FIELD_REF, field_name="revenue") for _ in range(3)
    ]

    # Literals
    hundred_million = ASTNode(
        node_type=NodeType.LITERAL, value=100000000, data_type=DataType.INTEGER
    )
    two = ASTNode(node_type=NodeType.LITERAL, value=2, data_type=DataType.INTEGER)
    half = ASTNode(node_type=NodeType.LITERAL, value=0.5, data_type=DataType.REAL)

    # String literals
    blockbuster = ASTNode(
        node_type=NodeType.LITERAL,
        value="Blockbuster Success",
        data_type=DataType.STRING,
    )
    flop = ASTNode(
        node_type=NodeType.LITERAL, value="Expensive Flop", data_type=DataType.STRING
    )
    indie_success = ASTNode(
        node_type=NodeType.LITERAL, value="Indie Success", data_type=DataType.STRING
    )
    low_budget = ASTNode(
        node_type=NodeType.LITERAL, value="Low Budget", data_type=DataType.STRING
    )

    # === OUTER CONDITION: [budget] > 100000000 ===
    outer_condition = ASTNode(
        node_type=NodeType.COMPARISON,
        operator=">",
        left=budget_refs[0],
        right=hundred_million,
    )

    # === INNER CONDITIONAL 1 (then branch): High budget movies ===
    # Condition: [revenue] > [budget] * 2
    budget_times_two = ASTNode(
        node_type=NodeType.ARITHMETIC, operator="*", left=budget_refs[1], right=two
    )

    inner_condition_1 = ASTNode(
        node_type=NodeType.COMPARISON,
        operator=">",
        left=revenue_refs[0],
        right=budget_times_two,
    )

    inner_conditional_1 = ASTNode(
        node_type=NodeType.CONDITIONAL,
        condition=inner_condition_1,
        then_branch=blockbuster,
        else_branch=flop,
    )

    # === INNER CONDITIONAL 2 (else branch): Low budget movies ===
    # Condition: ([revenue] - [budget]) / [budget] > 0.5 (profit margin > 50%)
    profit = ASTNode(
        node_type=NodeType.ARITHMETIC,
        operator="-",
        left=revenue_refs[1],
        right=budget_refs[2],
    )

    profit_margin = ASTNode(
        node_type=NodeType.ARITHMETIC, operator="/", left=profit, right=budget_refs[3]
    )

    inner_condition_2 = ASTNode(
        node_type=NodeType.COMPARISON, operator=">", left=profit_margin, right=half
    )

    inner_conditional_2 = ASTNode(
        node_type=NodeType.CONDITIONAL,
        condition=inner_condition_2,
        then_branch=indie_success,
        else_branch=low_budget,
    )

    # === OUTER CONDITIONAL: Combine everything ===
    outer_conditional = ASTNode(
        node_type=NodeType.CONDITIONAL,
        condition=outer_condition,
        then_branch=inner_conditional_1,  # Nested conditional!
        else_branch=inner_conditional_2,  # Another nested conditional!
    )

    return outer_conditional


def test_deeply_nested():
    """Test deeply nested conditional + arithmetic combinations."""
    print("🌳 **Deeply Nested Formula Test**\n")

    converter = ASTToLookMLConverter()

    # Create the complex AST
    complex_ast = create_movie_rating_formula()

    print("**Original Tableau Formula:**")
    print("```")
    print("IF [budget] > 100000000 THEN")
    print('    IF [revenue] > [budget] * 2 THEN "Blockbuster Success"')
    print('    ELSE "Expensive Flop"')
    print("ELSE")
    print('    IF ([revenue] - [budget]) / [budget] > 0.5 THEN "Indie Success"')
    print('    ELSE "Low Budget"')
    print("END")
    print("```\n")

    # Convert to LookML
    result = converter.convert_to_lookml(complex_ast, "TABLE")

    print("**Generated LookML SQL:**")
    print("```sql")
    print(result)
    print("```\n")

    print("🔍 **Recursion Breakdown:**")
    print("1. **Outer Conditional** (`_convert_conditional`)")
    print("   - Condition: `[budget] > 100000000` → comparison")
    print("   - Then: Inner conditional 1 → **RECURSION**")
    print("   - Else: Inner conditional 2 → **RECURSION**")
    print()
    print("2. **Inner Conditional 1** (`_convert_conditional` called recursively)")
    print("   - Condition: `[revenue] > [budget] * 2` → comparison with arithmetic")
    print("     - Left: `[revenue]` → field reference")
    print("     - Right: `[budget] * 2` → **arithmetic recursion**")
    print('   - Then: `"Blockbuster Success"` → literal')
    print('   - Else: `"Expensive Flop"` → literal')
    print()
    print("3. **Inner Conditional 2** (`_convert_conditional` called recursively)")
    print("   - Condition: `([revenue] - [budget]) / [budget] > 0.5` → comparison")
    print("     - Left: `([revenue] - [budget]) / [budget]` → **nested arithmetic**")
    print("       - Numerator: `[revenue] - [budget]` → arithmetic")
    print("       - Denominator: `[budget]` → field reference")
    print("     - Right: `0.5` → literal")
    print('   - Then: `"Indie Success"` → literal')
    print('   - Else: `"Low Budget"` → literal')
    print()
    print("🎯 **Key Points:**")
    print("- **No depth limit**: Recursion can go as deep as needed")
    print(
        "- **Mixed node types**: Conditionals contain arithmetic, comparisons contain arithmetic"
    )
    print("- **Clean SQL output**: All parentheses and operators preserved")
    print("- **Type safety**: Each node type has its own conversion logic")


def show_call_stack():
    """Show what the call stack looks like during conversion."""
    print("\n" + "=" * 60)
    print("📞 **Method Call Stack During Conversion:**\n")

    print("```python")
    print("convert_to_lookml(outer_conditional)")
    print("└── _convert_node(outer_conditional)")
    print("    └── _convert_conditional(outer_conditional)")
    print("        ├── _convert_node(condition)  # [budget] > 100000000")
    print("        │   └── _convert_comparison()")
    print("        │       ├── _convert_node([budget])  # Field ref")
    print("        │       └── _convert_node(100000000)  # Literal")
    print("        ├── _convert_node(then_branch)  # Inner conditional 1")
    print("        │   └── _convert_conditional(inner_conditional_1)")
    print("        │       ├── _convert_node(condition)  # [revenue] > [budget] * 2")
    print("        │       │   └── _convert_comparison()")
    print("        │       │       ├── _convert_node([revenue])  # Field ref")
    print("        │       │       └── _convert_node([budget] * 2)  # Arithmetic!")
    print("        │       │           └── _convert_arithmetic()")
    print("        │       │               ├── _convert_node([budget])  # Field ref")
    print("        │       │               └── _convert_node(2)  # Literal")
    print('        │       ├── _convert_node("Blockbuster Success")  # Literal')
    print('        │       └── _convert_node("Expensive Flop")  # Literal')
    print("        └── _convert_node(else_branch)  # Inner conditional 2")
    print("            └── _convert_conditional(inner_conditional_2)")
    print("                ├── _convert_node(complex_condition)  # Profit margin > 0.5")
    print("                │   └── _convert_comparison()")
    print("                │       ├── _convert_node(profit_margin)  # Division!")
    print("                │       │   └── _convert_arithmetic(division)")
    print("                │       │       ├── _convert_node(profit)  # Subtraction!")
    print("                │       │       │   └── _convert_arithmetic(subtraction)")
    print(
        "                │       │       │       ├── _convert_node([revenue])  # Field ref"
    )
    print(
        "                │       │       │       └── _convert_node([budget])  # Field ref"
    )
    print("                │       │       └── _convert_node([budget])  # Field ref")
    print("                │       └── _convert_node(0.5)  # Literal")
    print('                ├── _convert_node("Indie Success")  # Literal')
    print('                └── _convert_node("Low Budget")  # Literal')
    print("```")
    print()
    print("💡 **Each recursive call builds up the SQL expression bottom-up!**")


if __name__ == "__main__":
    test_deeply_nested()
    show_call_stack()
