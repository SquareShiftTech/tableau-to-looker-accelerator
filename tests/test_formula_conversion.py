#!/usr/bin/env python3
"""
Comprehensive tests for Tableau formula to LookML conversion.

Tests the end-to-end conversion from Tableau formulas to LookML SQL expressions.
"""

import pytest
from src.tableau_to_looker_parser.converters.formula_parser import FormulaParser
from src.tableau_to_looker_parser.converters.ast_to_lookml_converter import (
    ASTToLookMLConverter,
)


class TestFormulaConversion:
    """Test class for Tableau formula to LookML conversion."""

    @pytest.fixture
    def formula_parser(self):
        """Create a formula parser instance."""
        return FormulaParser()

    @pytest.fixture
    def ast_converter(self):
        """Create an AST to LookML converter instance."""
        return ASTToLookMLConverter()

    def convert_formula(self, tableau_formula: str, formula_parser, ast_converter):
        """Helper method to convert Tableau formula to LookML."""
        try:
            # Parse Tableau formula to AST
            parse_result = formula_parser.parse_formula(tableau_formula)
            if not parse_result.success:
                return f"PARSE_ERROR: {parse_result.error_message}"

            # Get AST from calculated field
            if (
                not parse_result.calculated_field
                or not parse_result.calculated_field.ast_root
            ):
                return "PARSE_ERROR: No AST generated"

            ast = parse_result.calculated_field.ast_root

            # Convert AST to LookML
            lookml_sql = ast_converter.convert_to_lookml(ast, "TABLE")
            return lookml_sql
        except Exception as e:
            return f"CONVERSION_ERROR: {str(e)}"

    # ============================================================================
    # BASIC FIELD REFERENCES
    # ============================================================================

    def test_field_references(self, formula_parser, ast_converter):
        """Test basic field reference conversion."""
        test_cases = [
            ("[Sales]", "${TABLE}.sales"),
            ("[Order Date]", "${TABLE}.order_date"),
            ("[Customer Name]", "${TABLE}.customer_name"),
            ("[Product ID]", "${TABLE}.product_id"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # AGGREGATION FUNCTIONS
    # ============================================================================

    def test_aggregation_functions(self, formula_parser, ast_converter):
        """Test aggregation function conversion."""
        test_cases = [
            ("SUM([Sales])", "SUM(${TABLE}.sales)"),
            ("COUNT([Orders])", "COUNT(${TABLE}.orders)"),
            ("AVG([Profit])", "AVG(${TABLE}.profit)"),
            ("MIN([Date])", "MIN(${TABLE}.date)"),
            ("MAX([Price])", "MAX(${TABLE}.price)"),
            ("MEDIAN([Revenue])", "MEDIAN(${TABLE}.revenue)"),
            # New functions from Excel mapping
            ("COUNTD([Customer ID])", "COUNT(DISTINCT ${TABLE}.customer_id)"),
            ("STDEV([Sales])", "STDDEV_SAMP(${TABLE}.sales)"),
            ("STDEVP([Sales])", "STDDEV_POP(${TABLE}.sales)"),
            ("VAR([Profit])", "VAR_SAMP(${TABLE}.profit)"),
            ("VARP([Profit])", "VAR_POP(${TABLE}.profit)"),
            ("CORR([Sales], [Profit])", "CORR(${TABLE}.sales, ${TABLE}.profit)"),
            ("COVAR([Sales], [Profit])", "COVAR_SAMP(${TABLE}.sales, ${TABLE}.profit)"),
            ("COVARP([Sales], [Profit])", "COVAR_POP(${TABLE}.sales, ${TABLE}.profit)"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # STRING FUNCTIONS
    # ============================================================================

    def test_string_functions(self, formula_parser, ast_converter):
        """Test string function conversion."""
        test_cases = [
            ("UPPER([Name])", "UPPER(${TABLE}.name)"),
            ("LOWER([Category])", "LOWER(${TABLE}.category)"),
            ("LEN([Title])", "LENGTH(${TABLE}.title)"),
            ("TRIM([Description])", "TRIM(${TABLE}.description)"),
            ("LEFT([Code], 3)", "LEFT(${TABLE}.code, 3)"),
            ("RIGHT([Code], 2)", "RIGHT(${TABLE}.code, 2)"),
            ("MID([Text], 2, 5)", "SUBSTR(${TABLE}.text, 2, 5)"),
            # Advanced string functions
            ("CONTAINS([Name], 'John')", "STRPOS(${TABLE}.name, 'John') > 0"),
            ("STARTSWITH([Code], 'A')", "STARTS_WITH(${TABLE}.code, 'A')"),
            (
                "ENDSWITH([File], '.pdf')",
                "ENDS_WITH(${TABLE}.file, '.pdf')",
            ),
            ("REPLACE([Text], 'old', 'new')", "REPLACE(${TABLE}.text, 'old', 'new')"),
            ("FIND([Text], 'word')", "STRPOS(${TABLE}.text, 'word')"),
            ("LTRIM([Text])", "LTRIM(${TABLE}.text)"),
            ("RTRIM([Text])", "RTRIM(${TABLE}.text)"),
            # New functions from Excel mapping
            ("ASCII([Char])", "ASCII(${TABLE}.char)"),
            ("CHAR(65)", "CHR(65)"),
            ("PROPER([Name])", "INITCAP(${TABLE}.name)"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # MATH FUNCTIONS
    # ============================================================================

    def test_math_functions(self, formula_parser, ast_converter):
        """Test mathematical function conversion."""
        test_cases = [
            ("ABS([Profit])", "ABS(${TABLE}.profit)"),
            ("ROUND([Sales], 2)", "ROUND(${TABLE}.sales, 2)"),
            ("CEILING([Price])", "CEIL(${TABLE}.price)"),
            ("FLOOR([Amount])", "FLOOR(${TABLE}.amount)"),
            ("SQRT([Value])", "SQRT(${TABLE}.value)"),
            ("POWER([Base], 2)", "POWER(${TABLE}.base, 2)"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # DATE FUNCTIONS
    # ============================================================================

    def test_date_functions(self, formula_parser, ast_converter):
        """Test date function conversion."""
        test_cases = [
            ("YEAR([Order Date])", "EXTRACT(YEAR FROM ${TABLE}.order_date)"),
            ("MONTH([Order Date])", "EXTRACT(MONTH FROM ${TABLE}.order_date)"),
            ("DAY([Order Date])", "EXTRACT(DAY FROM ${TABLE}.order_date)"),
            ("NOW()", "CURRENT_TIMESTAMP"),
            ("TODAY()", "CURRENT_DATE"),
            # New date functions from Excel mapping
            ("DATEADD([Date], 30, 'day')", "DATE_ADD(${TABLE}.date, INTERVAL 30 day)"),
            (
                "DATEDIFF([Start Date], [End Date], 'day')",
                "DATE_DIFF(${TABLE}.end_date, ${TABLE}.start_date, day)",
            ),
            ("DATETRUNC([Date], 'month')", "DATE_TRUNC(${TABLE}.date, month)"),
            ("PARSE_DATE('2023-01-01')", "PARSE_DATE('%Y-%m-%d', '2023-01-01')"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # TYPE CONVERSION FUNCTIONS
    # ============================================================================

    def test_type_conversion_functions(self, formula_parser, ast_converter):
        """Test type conversion function conversion."""
        test_cases = [
            ("FLOAT([Text Value])", "CAST(${TABLE}.text_value AS FLOAT64)"),
            ("INT([Decimal Value])", "CAST(${TABLE}.decimal_value AS INT64)"),
            ("STR([Number])", "CAST(${TABLE}.number AS STRING)"),
            ("DATE([Text Date])", "DATE(${TABLE}.text_date)"),
            ("DATETIME([Text DateTime])", "DATETIME(${TABLE}.text_datetime)"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # LOGICAL FUNCTIONS
    # ============================================================================

    def test_logical_functions(self, formula_parser, ast_converter):
        """Test logical function conversion."""
        test_cases = [
            ("IFNULL([Value], 0)", "IFNULL(${TABLE}.value, 0)"),
            ("ISNULL([Field])", "${TABLE}.field IS NULL"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # ARITHMETIC OPERATIONS
    # ============================================================================

    def test_arithmetic_operations(self, formula_parser, ast_converter):
        """Test arithmetic operation conversion."""
        test_cases = [
            ("[Sales] + [Profit]", "(${TABLE}.sales + ${TABLE}.profit)"),
            ("[Revenue] - [Cost]", "(${TABLE}.revenue - ${TABLE}.cost)"),
            ("[Price] * [Quantity]", "(${TABLE}.price * ${TABLE}.quantity)"),
            ("[Total] / [Count]", "(${TABLE}.total / ${TABLE}.count)"),
            ("[Base] ^ 2", "POWER(${TABLE}.base, 2)"),
            ("[Value] % 10", "MOD(${TABLE}.value, 10)"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # COMPARISON OPERATIONS
    # ============================================================================

    def test_comparison_operations(self, formula_parser, ast_converter):
        """Test comparison operation conversion."""
        test_cases = [
            ("[Sales] > 1000", "(${TABLE}.sales > 1000)"),
            ("[Profit] < 0", "(${TABLE}.profit < 0)"),
            ("[Status] = 'Active'", "(${TABLE}.status = 'Active')"),
            ("[Value] != NULL", "(${TABLE}.value != NULL)"),
            ("[Score] >= 80", "(${TABLE}.score >= 80)"),
            ("[Rating] <= 5", "(${TABLE}.rating <= 5)"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # LOGICAL OPERATIONS
    # ============================================================================

    def test_logical_operations(self, formula_parser, ast_converter):
        """Test logical operation conversion."""
        test_cases = [
            (
                "[Sales] > 1000 AND [Profit] > 0",
                "((${TABLE}.sales > 1000) AND (${TABLE}.profit > 0))",
            ),
            (
                "[Status] = 'A' OR [Status] = 'B'",
                "((${TABLE}.status = 'A') OR (${TABLE}.status = 'B'))",
            ),
            ("NOT [Active]", "NOT ${TABLE}.active"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # CONDITIONAL EXPRESSIONS (IF-THEN-ELSE)
    # ============================================================================

    def test_conditional_expressions(self, formula_parser, ast_converter):
        """Test IF-THEN-ELSE conversion to CASE-WHEN."""
        test_cases = [
            (
                "IF [Sales] > 1000 THEN 'High' ELSE 'Low' END",
                "CASE WHEN (${TABLE}.sales > 1000) THEN 'High' ELSE 'Low' END",
            ),
            (
                "IF [Profit] > 0 THEN 'Profitable' ELSE 'Loss' END",
                "CASE WHEN (${TABLE}.profit > 0) THEN 'Profitable' ELSE 'Loss' END",
            ),
            (
                "IF [Category] = 'A' THEN 1 ELSE 0 END",
                "CASE WHEN (${TABLE}.category = 'A') THEN 1 ELSE 0 END",
            ),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # COMPLEX FORMULAS
    # ============================================================================

    def test_complex_formulas(self, formula_parser, ast_converter):
        """Test complex multi-function formulas."""
        test_cases = [
            # Nested functions
            ("UPPER(LEFT([Name], 5))", "UPPER(LEFT(${TABLE}.name, 5))"),
            # Function with arithmetic
            ("SUM([Sales] * [Quantity])", "SUM((${TABLE}.sales * ${TABLE}.quantity))"),
            # Complex conditional with functions
            (
                "IF LEN([Code]) > 5 THEN UPPER([Code]) ELSE LOWER([Code]) END",
                "CASE WHEN (LENGTH(${TABLE}.code) > 5) THEN UPPER(${TABLE}.code) ELSE LOWER(${TABLE}.code) END",
            ),
            # Multiple operations
            (
                "ROUND([Sales] / [Quantity], 2)",
                "ROUND((${TABLE}.sales / ${TABLE}.quantity), 2)",
            ),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    # ============================================================================
    # EDGE CASES AND ERROR HANDLING
    # ============================================================================

    def test_literals(self, formula_parser, ast_converter):
        """Test literal value conversion."""
        test_cases = [
            ("'Hello World'", "'Hello World'"),
            ("123", "123"),
            ("45.67", "45.67"),
            ("TRUE", "TRUE"),
            ("FALSE", "FALSE"),
            ("NULL", "NULL"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for {tableau_formula}: got {result}"
            )

    def test_function_registry_coverage(self, ast_converter):
        """Test that all functions in registry are accessible."""
        expected_functions = {
            # Aggregation
            "SUM",
            "COUNT",
            "AVG",
            "MIN",
            "MAX",
            "MEDIAN",
            "COUNTD",
            "STDEV",
            "STDEVP",
            "CORR",
            "COVAR",
            "COVARP",
            "VAR",
            "VARP",
            # String
            "UPPER",
            "LOWER",
            "LEN",
            "TRIM",
            "LEFT",
            "RIGHT",
            "MID",
            "CONTAINS",
            "STARTSWITH",
            "ENDSWITH",
            "REPLACE",
            "FIND",
            "SPLIT",
            "LTRIM",
            "RTRIM",
            "ASCII",
            "CHAR",
            "PROPER",
            # Math
            "ABS",
            "ROUND",
            "CEILING",
            "FLOOR",
            "SQRT",
            "POWER",
            # Date
            "YEAR",
            "MONTH",
            "DAY",
            "NOW",
            "TODAY",
            "DATEADD",
            "DATEDIFF",
            "DATETRUNC",
            "PARSE_DATE",
            # Type conversion
            "FLOAT",
            "INT",
            "STR",
            "DATE",
            "DATETIME",
            # Logical
            "IFNULL",
            "ISNULL",
        }

        registry_functions = set(ast_converter.function_registry.keys())

        # Check that all expected functions are present
        missing_functions = expected_functions - registry_functions
        assert len(missing_functions) == 0, (
            f"Missing functions in registry: {missing_functions}"
        )

        # Check that we don't have unexpected functions
        # Allow some extra functions that might be added
        assert len(registry_functions) >= len(expected_functions), (
            "Registry has fewer functions than expected"
        )

    def test_error_handling(self, formula_parser, ast_converter):
        """Test error handling for invalid formulas."""
        invalid_formulas = [
            "",  # Empty string
            "INVALID_FUNCTION([Field])",  # Unknown function
            "[",  # Malformed field reference
            "SUM(",  # Incomplete function call
        ]

        for invalid_formula in invalid_formulas:
            result = self.convert_formula(
                invalid_formula, formula_parser, ast_converter
            )
            # Should either parse successfully or return an error message
            assert isinstance(result, str), (
                f"Should return string result for {invalid_formula}"
            )
            # Error cases should contain either error text or valid LookML
            if "ERROR" not in result:
                # If no error, should be valid LookML (contains ${TABLE} or simple values)
                assert "${TABLE}" in result or result in ["NULL", "''"], (
                    f"Unexpected result for {invalid_formula}: {result}"
                )


# ============================================================================
# INTEGRATION TESTS WITH REAL TABLEAU FORMULAS
# ============================================================================


class TestRealTableauFormulas:
    """Test with real-world Tableau formulas from sample files."""

    @pytest.fixture
    def formula_parser(self):
        return FormulaParser()

    @pytest.fixture
    def ast_converter(self):
        return ASTToLookMLConverter()

    def convert_formula(self, tableau_formula: str, formula_parser, ast_converter):
        """Helper method to convert Tableau formula to LookML."""
        try:
            parse_result = formula_parser.parse_formula(tableau_formula)
            if not parse_result.success:
                return f"PARSE_ERROR: {parse_result.error_message}"

            # Get AST from calculated field
            if (
                not parse_result.calculated_field
                or not parse_result.calculated_field.ast_root
            ):
                return "PARSE_ERROR: No AST generated"

            ast = parse_result.calculated_field.ast_root

            lookml_sql = ast_converter.convert_to_lookml(ast, "TABLE")
            return lookml_sql
        except Exception as e:
            return f"CONVERSION_ERROR: {str(e)}"

    def test_sample_calculated_fields(self, formula_parser, ast_converter):
        """Test with calculated fields from sample Tableau files."""
        # These are based on actual calculated fields from the sample TWB files
        real_formulas = [
            # Basic calculations
            ("SUM([Sales])", "SUM(${TABLE}.sales)"),
            ("AVG([Profit Ratio])", "AVG(${TABLE}.profit_ratio)"),
            # String manipulations
            ("UPPER([Category])", "UPPER(${TABLE}.category)"),
            ("LEFT([Product Name], 10)", "LEFT(${TABLE}.product_name, 10)"),
            # Conditionals from real usage
            (
                "IF [Profit] > 0 THEN 'Profitable' ELSE 'Loss' END",
                "CASE WHEN (${TABLE}.profit > 0) THEN 'Profitable' ELSE 'Loss' END",
            ),
            # Complex calculations
            (
                "ROUND([Sales] / [Quantity], 2)",
                "ROUND((${TABLE}.sales / ${TABLE}.quantity), 2)",
            ),
            # Date calculations
            ("YEAR([Order Date])", "EXTRACT(YEAR FROM ${TABLE}.order_date)"),
            (
                "DATEDIFF([Ship Date], [Order Date], 'day')",
                "DATE_DIFF(${TABLE}.order_date, ${TABLE}.ship_date, day)",
            ),
        ]

        for tableau_formula, expected_lookml in real_formulas:
            result = self.convert_formula(
                tableau_formula, formula_parser, ast_converter
            )
            assert result == expected_lookml, (
                f"Failed for real formula {tableau_formula}: got {result}"
            )


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "--tb=short"])
