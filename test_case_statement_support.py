#!/usr/bin/env python3
"""
Test CASE statement support in AST to LookML converter.
"""

from tableau_to_looker_parser.converters.ast_to_lookml_converter import (
    ASTToLookMLConverter,
)
from tableau_to_looker_parser.models.ast_schema import (
    ASTNode,
    NodeType,
    DataType,
    WhenClause,
)


def create_simple_case_example():
    """
    Create a simple CASE statement:
    CASE [category] WHEN "Electronics" THEN 0.1 WHEN "Books" THEN 0.05 ELSE 0 END
    """

    # Case expression: [category]
    case_expression = ASTNode(node_type=NodeType.FIELD_REF, field_name="category")

    # WHEN clauses
    when_clauses = [
        # WHEN "Electronics" THEN 0.1
        WhenClause(
            condition=ASTNode(
                node_type=NodeType.LITERAL,
                value="Electronics",
                data_type=DataType.STRING,
            ),
            result=ASTNode(
                node_type=NodeType.LITERAL, value=0.1, data_type=DataType.REAL
            ),
        ),
        # WHEN "Books" THEN 0.05
        WhenClause(
            condition=ASTNode(
                node_type=NodeType.LITERAL, value="Books", data_type=DataType.STRING
            ),
            result=ASTNode(
                node_type=NodeType.LITERAL, value=0.05, data_type=DataType.REAL
            ),
        ),
    ]

    # ELSE 0
    else_branch = ASTNode(
        node_type=NodeType.LITERAL, value=0, data_type=DataType.INTEGER
    )

    # Complete CASE statement
    case_ast = ASTNode(
        node_type=NodeType.CASE,
        case_expression=case_expression,
        when_clauses=when_clauses,
        else_branch=else_branch,
    )

    return case_ast


def create_searched_case_example():
    """
    Create a searched CASE statement:
    CASE WHEN [sales] > 1000 THEN "High" WHEN [sales] > 500 THEN "Medium" ELSE "Low" END
    """

    # Field references
    sales_ref_1 = ASTNode(node_type=NodeType.FIELD_REF, field_name="sales")
    sales_ref_2 = ASTNode(node_type=NodeType.FIELD_REF, field_name="sales")

    # Literals
    thousand = ASTNode(
        node_type=NodeType.LITERAL, value=1000, data_type=DataType.INTEGER
    )
    five_hundred = ASTNode(
        node_type=NodeType.LITERAL, value=500, data_type=DataType.INTEGER
    )
    high = ASTNode(node_type=NodeType.LITERAL, value="High", data_type=DataType.STRING)
    medium = ASTNode(
        node_type=NodeType.LITERAL, value="Medium", data_type=DataType.STRING
    )
    low = ASTNode(node_type=NodeType.LITERAL, value="Low", data_type=DataType.STRING)

    # WHEN clauses with conditions
    when_clauses = [
        # WHEN [sales] > 1000 THEN "High"
        WhenClause(
            condition=ASTNode(
                node_type=NodeType.COMPARISON,
                operator=">",
                left=sales_ref_1,
                right=thousand,
            ),
            result=high,
        ),
        # WHEN [sales] > 500 THEN "Medium"
        WhenClause(
            condition=ASTNode(
                node_type=NodeType.COMPARISON,
                operator=">",
                left=sales_ref_2,
                right=five_hundred,
            ),
            result=medium,
        ),
    ]

    # Complete searched CASE statement (no case_expression)
    case_ast = ASTNode(
        node_type=NodeType.CASE,
        case_expression=None,  # No case expression = searched CASE
        when_clauses=when_clauses,
        else_branch=low,
    )

    return case_ast


def create_complex_nested_case():
    """
    Create a complex CASE with nested arithmetic:
    CASE WHEN ([revenue] - [budget]) / [budget] > 0.5 THEN "Profitable"
         WHEN [revenue] > [budget] THEN "Break Even"
         ELSE "Loss"
    END
    """

    # Field references (need multiple copies)
    revenue_1 = ASTNode(node_type=NodeType.FIELD_REF, field_name="revenue")
    budget_1 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")
    budget_2 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")
    revenue_2 = ASTNode(node_type=NodeType.FIELD_REF, field_name="revenue")
    budget_3 = ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")

    # Literals
    half = ASTNode(node_type=NodeType.LITERAL, value=0.5, data_type=DataType.REAL)
    profitable = ASTNode(
        node_type=NodeType.LITERAL, value="Profitable", data_type=DataType.STRING
    )
    break_even = ASTNode(
        node_type=NodeType.LITERAL, value="Break Even", data_type=DataType.STRING
    )
    loss = ASTNode(node_type=NodeType.LITERAL, value="Loss", data_type=DataType.STRING)

    # Complex condition 1: ([revenue] - [budget]) / [budget] > 0.5
    profit = ASTNode(
        node_type=NodeType.ARITHMETIC, operator="-", left=revenue_1, right=budget_1
    )

    profit_margin = ASTNode(
        node_type=NodeType.ARITHMETIC, operator="/", left=profit, right=budget_2
    )

    condition_1 = ASTNode(
        node_type=NodeType.COMPARISON, operator=">", left=profit_margin, right=half
    )

    # Simple condition 2: [revenue] > [budget]
    condition_2 = ASTNode(
        node_type=NodeType.COMPARISON, operator=">", left=revenue_2, right=budget_3
    )

    # WHEN clauses
    when_clauses = [
        WhenClause(condition=condition_1, result=profitable),
        WhenClause(condition=condition_2, result=break_even),
    ]

    # Complete complex CASE
    case_ast = ASTNode(
        node_type=NodeType.CASE,
        case_expression=None,  # Searched CASE
        when_clauses=when_clauses,
        else_branch=loss,
    )

    return case_ast


def test_case_statements():
    """Test all types of CASE statements."""
    print("🔄 **Testing CASE Statement Support**\n")

    converter = ASTToLookMLConverter()

    # Test 1: Simple CASE
    print("1️⃣ **Simple CASE Statement**")
    simple_case = create_simple_case_example()

    print("**Tableau:**")
    print(
        'CASE [category] WHEN "Electronics" THEN 0.1 WHEN "Books" THEN 0.05 ELSE 0 END\n'
    )

    result = converter.convert_to_lookml(simple_case, "TABLE")
    print("**LookML:**")
    print(f"{result}\n")
    print("✅ Simple CASE with field expression and literal values\n")

    # Test 2: Searched CASE
    print("2️⃣ **Searched CASE Statement**")
    searched_case = create_searched_case_example()

    print("**Tableau:**")
    print(
        'CASE WHEN [sales] > 1000 THEN "High" WHEN [sales] > 500 THEN "Medium" ELSE "Low" END\n'
    )

    result = converter.convert_to_lookml(searched_case, "TABLE")
    print("**LookML:**")
    print(f"{result}\n")
    print("✅ Searched CASE with comparison conditions\n")

    # Test 3: Complex nested CASE
    print("3️⃣ **Complex CASE with Nested Arithmetic**")
    complex_case = create_complex_nested_case()

    print("**Tableau:**")
    print('CASE WHEN ([revenue] - [budget]) / [budget] > 0.5 THEN "Profitable"')
    print('     WHEN [revenue] > [budget] THEN "Break Even"')
    print('     ELSE "Loss"')
    print("END\n")

    result = converter.convert_to_lookml(complex_case, "TABLE")
    print("**LookML:**")
    print(f"{result}\n")
    print("✅ Complex CASE with nested arithmetic in conditions\n")


def explain_case_vs_if():
    """Explain the difference between CASE and IF-THEN-ELSE."""
    print("=" * 60)
    print("📚 **CASE vs IF-THEN-ELSE in Tableau**\n")

    print("**IF-THEN-ELSE**: Binary choice (single condition)")
    print("- IF condition THEN result1 ELSE result2 END")
    print("- Good for: Two-way branching\n")

    print("**CASE**: Multiple choices (multiple conditions)")
    print(
        "- Simple CASE: CASE [field] WHEN value1 THEN result1 WHEN value2 THEN result2 ELSE default END"
    )
    print(
        "- Searched CASE: CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 ELSE default END"
    )
    print("- Good for: Multi-way branching, cleaner than nested IF statements\n")

    print("**Both are now supported** by our AST to LookML converter! 🎉")
    print()
    print("**Conversion Examples:**")
    print("- Tableau IF → LookML: CASE WHEN ... THEN ... ELSE ... END")
    print("- Tableau Simple CASE → LookML: CASE field WHEN ... THEN ... ELSE ... END")
    print(
        "- Tableau Searched CASE → LookML: CASE WHEN ... THEN ... WHEN ... THEN ... ELSE ... END"
    )


if __name__ == "__main__":
    test_case_statements()
    explain_case_vs_if()
