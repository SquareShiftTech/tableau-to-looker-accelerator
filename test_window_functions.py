"""
Comprehensive test suite for window functions implementation.

Tests the complete pipeline: Tableau formulas → AST → LookML SQL generation.
"""

from src.tableau_to_looker_parser.converters.formula_parser import FormulaParser
from src.tableau_to_looker_parser.converters.ast_to_lookml_converter import (
    ASTToLookMLConverter,
)
from src.tableau_to_looker_parser.models.ast_schema import NodeType


class TestWindowFunctions:
    """Test window function parsing and LookML conversion."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = FormulaParser()
        self.converter = ASTToLookMLConverter()

    def test_running_sum_parsing(self):
        """Test RUNNING_SUM formula parsing."""
        formula = "RUNNING_SUM([Sales])"
        result = self.parser.parse_formula(formula)

        assert result.success
        assert result.calculated_field is not None
        assert result.calculated_field.ast_root.node_type == NodeType.WINDOW_FUNCTION
        assert result.calculated_field.ast_root.window_function_type == "RUNNING_SUM"
        assert len(result.calculated_field.ast_root.arguments) == 1
        assert (
            result.calculated_field.ast_root.arguments[0].node_type
            == NodeType.FIELD_REF
        )
        assert result.calculated_field.ast_root.arguments[0].field_name == "sales"

    def test_running_sum_lookml_conversion(self):
        """Test RUNNING_SUM LookML generation."""
        formula = "RUNNING_SUM([Sales])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "SUM(${TABLE}.sales) OVER (ORDER BY ${TABLE}.sales)"

        assert lookml_sql == expected

    def test_running_avg_lookml_conversion(self):
        """Test RUNNING_AVG LookML generation."""
        formula = "RUNNING_AVG([Profit])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "AVG(${TABLE}.profit) OVER (ORDER BY ${TABLE}.profit)"

        assert lookml_sql == expected

    def test_running_count_lookml_conversion(self):
        """Test RUNNING_COUNT LookML generation."""
        formula = "RUNNING_COUNT([Orders])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "COUNT(${TABLE}.orders) OVER (ORDER BY ${TABLE}.orders)"

        assert lookml_sql == expected

    def test_rank_default_order(self):
        """Test RANK with default ascending order."""
        formula = "RANK([Sales])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "RANK() OVER (ORDER BY ${TABLE}.sales ASC)"

        assert lookml_sql == expected

    def test_rank_descending_order(self):
        """Test RANK with descending order."""
        formula = "RANK([Sales], 'desc')"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "RANK() OVER (ORDER BY ${TABLE}.sales DESC)"

        assert lookml_sql == expected

    def test_dense_rank_descending(self):
        """Test DENSE_RANK with descending order."""
        formula = "DENSE_RANK([Revenue], 'desc')"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "DENSE_RANK() OVER (ORDER BY ${TABLE}.revenue DESC)"

        assert lookml_sql == expected

    def test_row_number(self):
        """Test ROW_NUMBER function."""
        formula = "ROW_NUMBER()"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))"

        assert lookml_sql == expected

    def test_window_sum_with_range(self):
        """Test WINDOW_SUM with range parameters."""
        formula = "WINDOW_SUM([Sales], -2, 0)"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "SUM(${TABLE}.sales) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"

        assert lookml_sql == expected

    def test_window_avg_with_range(self):
        """Test WINDOW_AVG with range parameters."""
        formula = "WINDOW_AVG([Profit], -1, 1)"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = (
            "AVG(${TABLE}.profit) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
        )

        assert lookml_sql == expected

    def test_lag_function(self):
        """Test LAG function with default parameters."""
        formula = "LAG([Sales])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "LAG(${TABLE}.sales, 1, NULL) OVER (ORDER BY ${TABLE}.sales)"

        assert lookml_sql == expected

    def test_lag_function_with_offset(self):
        """Test LAG function with custom offset and default."""
        formula = "LAG([Sales], 2, 0)"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "LAG(${TABLE}.sales, 2, 0) OVER (ORDER BY ${TABLE}.sales)"

        assert lookml_sql == expected

    def test_lead_function(self):
        """Test LEAD function."""
        formula = "LEAD([Sales], 1, 0)"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "LEAD(${TABLE}.sales, 1, 0) OVER (ORDER BY ${TABLE}.sales)"

        assert lookml_sql == expected

    def test_complex_window_expression(self):
        """Test window function in complex expression."""
        formula = "IF RUNNING_SUM([Sales]) > 1000 THEN 'High' ELSE 'Low' END"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        # Should be a conditional with window function in condition
        assert ast_root.node_type == NodeType.CONDITIONAL
        assert ast_root.condition.node_type == NodeType.COMPARISON
        assert ast_root.condition.left.node_type == NodeType.WINDOW_FUNCTION
        assert ast_root.condition.left.window_function_type == "RUNNING_SUM"

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "CASE WHEN (SUM(${TABLE}.sales) OVER (ORDER BY ${TABLE}.sales) > 1000) THEN 'High' ELSE 'Low' END"

        assert lookml_sql == expected

    def test_nested_window_functions(self):
        """Test arithmetic with multiple window functions."""
        formula = "RUNNING_SUM([Sales]) - LAG([Sales])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        # Should be arithmetic with two window functions
        assert ast_root.node_type == NodeType.ARITHMETIC
        assert ast_root.operator == "-"
        assert ast_root.left.node_type == NodeType.WINDOW_FUNCTION
        assert ast_root.left.window_function_type == "RUNNING_SUM"
        assert ast_root.right.node_type == NodeType.WINDOW_FUNCTION
        assert ast_root.right.window_function_type == "LAG"

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "(SUM(${TABLE}.sales) OVER (ORDER BY ${TABLE}.sales) - LAG(${TABLE}.sales, 1, NULL) OVER (ORDER BY ${TABLE}.sales))"

        assert lookml_sql == expected

    def test_window_function_parsing_errors(self):
        """Test error handling for malformed window functions."""
        # Test with wrong number of arguments
        formula = "WINDOW_SUM([Sales])"  # Missing range parameters
        result = self.parser.parse_formula(formula)

        # Should still parse successfully but generate warning
        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        assert "wrong argument count" in lookml_sql

    def test_function_registry_integration(self):
        """Test that window functions are properly registered."""
        registry = self.parser.function_registry

        # Check that key window functions are registered
        assert registry.is_supported("RUNNING_SUM")
        assert registry.is_supported("RANK")
        assert registry.is_supported("WINDOW_SUM")
        assert registry.is_supported("LAG")
        assert registry.is_supported("LEAD")

        # Check function metadata
        running_sum = registry.get_function("RUNNING_SUM")
        assert running_sum.category == "window"
        assert running_sum.requires_context
        assert running_sum.min_args == 1
        assert running_sum.max_args == 1

    def test_end_to_end_window_pipeline(self):
        """Test complete window function pipeline with realistic formula."""
        formula = "IF RANK([Sales], 'desc') <= 10 THEN 'Top 10' ELSE 'Other' END"

        # Parse formula
        result = self.parser.parse_formula(formula, "sales_rank_category", "dimension")

        assert result.success
        assert result.calculated_field is not None
        assert result.calculated_field.name == "sales_rank_category"
        assert result.calculated_field.field_type == "dimension"
        assert result.calculated_field.original_formula == formula

        # Convert to LookML
        ast_root = result.calculated_field.ast_root
        lookml_sql = self.converter.convert_to_lookml(ast_root)

        expected = "CASE WHEN (RANK() OVER (ORDER BY ${TABLE}.sales DESC) <= 10) THEN 'Top 10' ELSE 'Other' END"
        assert lookml_sql == expected

        # Check dependencies
        assert "sales" in result.calculated_field.dependencies

        # Check complexity analysis
        assert result.calculated_field.complexity in ["medium", "complex"]


if __name__ == "__main__":
    # Run specific tests for debugging
    test_instance = TestWindowFunctions()
    test_instance.setup_method()

    print("Testing RUNNING_SUM parsing...")
    test_instance.test_running_sum_parsing()
    print("PASS - RUNNING_SUM parsing test passed")

    print("Testing RUNNING_SUM LookML conversion...")
    test_instance.test_running_sum_lookml_conversion()
    print("PASS - RUNNING_SUM LookML conversion test passed")

    print("Testing RANK with desc order...")
    test_instance.test_rank_descending_order()
    print("PASS - RANK desc test passed")

    print("Testing WINDOW_SUM with range...")
    test_instance.test_window_sum_with_range()
    print("PASS - WINDOW_SUM range test passed")

    print("Testing complex window expression...")
    test_instance.test_complex_window_expression()
    print("PASS - Complex window expression test passed")

    print("Testing function registry integration...")
    test_instance.test_function_registry_integration()
    print("PASS - Function registry integration test passed")

    print("Testing end-to-end pipeline...")
    test_instance.test_end_to_end_window_pipeline()
    print("PASS - End-to-end pipeline test passed")

    print("\nAll window function tests passed!")
