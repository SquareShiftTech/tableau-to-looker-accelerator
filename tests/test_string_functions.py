"""
Comprehensive test suite for advanced string functions implementation.

Tests the complete pipeline: Tableau formulas → AST → LookML SQL generation.
"""

from src.tableau_to_looker_parser.converters.formula_parser import FormulaParser
from src.tableau_to_looker_parser.converters.ast_to_lookml_converter import (
    ASTToLookMLConverter,
)
from src.tableau_to_looker_parser.models.ast_schema import NodeType


class TestStringFunctions:
    """Test advanced string function parsing and LookML conversion."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = FormulaParser()
        self.converter = ASTToLookMLConverter()

    def test_contains_function_parsing(self):
        """Test CONTAINS function parsing."""
        formula = "CONTAINS([Product Name], 'Widget')"
        result = self.parser.parse_formula(formula)

        assert result.success
        assert result.calculated_field is not None
        assert result.calculated_field.ast_root.node_type == NodeType.FUNCTION
        assert result.calculated_field.ast_root.function_name == "CONTAINS"
        assert len(result.calculated_field.ast_root.arguments) == 2

    def test_contains_function_lookml_conversion(self):
        """Test CONTAINS function LookML generation."""
        formula = "CONTAINS([Product Name], 'Widget')"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "STRPOS(${TABLE}.product_name, 'Widget') > 0"

        assert lookml_sql == expected

    def test_startswith_function_lookml_conversion(self):
        """Test STARTSWITH function LookML generation."""
        formula = "STARTSWITH([Customer Name], 'John')"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "STARTS_WITH(${TABLE}.customer_name, 'John')"

        assert lookml_sql == expected

    def test_endswith_function_lookml_conversion(self):
        """Test ENDSWITH function LookML generation."""
        formula = "ENDSWITH([Product Name], 'Pro')"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "ENDS_WITH(${TABLE}.product_name, 'Pro')"

        assert lookml_sql == expected

    def test_replace_function_lookml_conversion(self):
        """Test REPLACE function LookML generation."""
        formula = "REPLACE([Product Name], 'Widget', 'Gadget')"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "REPLACE(${TABLE}.product_name, 'Widget', 'Gadget')"

        assert lookml_sql == expected

    def test_find_function_lookml_conversion(self):
        """Test FIND function LookML generation."""
        formula = "FIND([Product Name], 'Widget')"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "STRPOS(${TABLE}.product_name, 'Widget')"

        assert lookml_sql == expected

    def test_split_function_lookml_conversion(self):
        """Test SPLIT function LookML generation."""
        formula = "SPLIT([Customer Name], ' ', 1)"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "SPLIT(${TABLE}.customer_name, ' ')[OFFSET(CASE WHEN 1 < 0 THEN 1 ELSE 1 - 1 END)]"

        assert lookml_sql == expected

    def test_ltrim_function_lookml_conversion(self):
        """Test LTRIM function LookML generation."""
        formula = "LTRIM([Product Name])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "LTRIM(${TABLE}.product_name)"

        assert lookml_sql == expected

    def test_rtrim_function_lookml_conversion(self):
        """Test RTRIM function LookML generation."""
        formula = "RTRIM([Product Name])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "RTRIM(${TABLE}.product_name)"

        assert lookml_sql == expected

    def test_complex_string_expression(self):
        """Test complex expression with multiple string functions."""
        formula = "IF CONTAINS(UPPER([Product Name]), 'WIDGET') THEN 'Widget Product' ELSE 'Other' END"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        # Should be a conditional with nested function calls
        assert ast_root.node_type == NodeType.CONDITIONAL
        assert ast_root.condition.node_type == NodeType.FUNCTION
        assert ast_root.condition.function_name == "CONTAINS"

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "CASE WHEN STRPOS(UPPER(${TABLE}.product_name), 'WIDGET') > 0 THEN 'Widget Product' ELSE 'Other' END"

        assert lookml_sql == expected

    def test_nested_string_functions(self):
        """Test nested string function calls."""
        formula = "REPLACE(UPPER([Product Name]), 'WIDGET', 'GADGET')"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        # Should be a REPLACE function with UPPER as first argument
        assert ast_root.node_type == NodeType.FUNCTION
        assert ast_root.function_name == "REPLACE"
        assert ast_root.arguments[0].node_type == NodeType.FUNCTION
        assert ast_root.arguments[0].function_name == "UPPER"

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "REPLACE(UPPER(${TABLE}.product_name), 'WIDGET', 'GADGET')"

        assert lookml_sql == expected

    def test_string_function_with_field_references(self):
        """Test string function using multiple field references."""
        formula = "CONTAINS([Product Name], [Category])"
        result = self.parser.parse_formula(formula)

        assert result.success
        ast_root = result.calculated_field.ast_root

        # Both arguments should be field references
        assert ast_root.arguments[0].node_type == NodeType.FIELD_REF
        assert ast_root.arguments[1].node_type == NodeType.FIELD_REF

        lookml_sql = self.converter.convert_to_lookml(ast_root)
        expected = "STRPOS(${TABLE}.product_name, ${TABLE}.category) > 0"

        assert lookml_sql == expected

    def test_string_function_error_handling(self):
        """Test error handling for string functions with wrong argument count."""
        formula = "CONTAINS([Product Name])"  # Missing second argument
        result = self.parser.parse_formula(formula)

        # Parser successfully parses with incomplete arguments
        assert result.success
        ast_root = result.calculated_field.ast_root

        # LookML converter should handle the missing argument gracefully
        lookml_sql = self.converter.convert_to_lookml(ast_root)

        # Should contain template error due to missing argument
        assert "template error" in lookml_sql

    def test_function_registry_integration(self):
        """Test that string functions are properly registered."""
        registry = self.parser.function_registry

        # Check that string functions are registered
        assert registry.is_supported("CONTAINS")
        assert registry.is_supported("STARTSWITH")
        assert registry.is_supported("ENDSWITH")
        assert registry.is_supported("REPLACE")
        assert registry.is_supported("FIND")
        assert registry.is_supported("SPLIT")
        assert registry.is_supported("LTRIM")
        assert registry.is_supported("RTRIM")

        # Check function metadata
        contains = registry.get_function("CONTAINS")
        assert contains.category == "string"
        assert contains.return_type == "boolean"
        assert contains.min_args == 2
        assert contains.max_args == 2

    def test_end_to_end_string_pipeline(self):
        """Test complete string function pipeline with realistic formula."""
        formula = "IF STARTSWITH(LTRIM([Customer Name]), 'VIP') THEN 'VIP Customer' ELSE 'Regular' END"

        # Parse formula
        result = self.parser.parse_formula(formula, "customer_type", "dimension")

        assert result.success
        assert result.calculated_field is not None
        assert result.calculated_field.name == "customer_type"
        assert result.calculated_field.field_type == "dimension"

        # Convert to LookML
        ast_root = result.calculated_field.ast_root
        lookml_sql = self.converter.convert_to_lookml(ast_root)

        expected = "CASE WHEN STARTS_WITH(LTRIM(${TABLE}.customer_name), 'VIP') THEN 'VIP Customer' ELSE 'Regular' END"
        assert lookml_sql == expected

        # Check dependencies
        assert "customer_name" in result.calculated_field.dependencies

        # Check complexity analysis
        assert result.calculated_field.complexity in ["medium", "complex"]
