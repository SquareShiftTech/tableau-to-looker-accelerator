#!/usr/bin/env python3
"""
Comprehensive tests for Tableau formula to LookML conversion.

Tests the end-to-end conversion from Tableau formulas to LookML SQL expressions.
"""

import pytest
from tableau_to_looker_parser.converters.formula_parser import FormulaParser
from tableau_to_looker_parser.converters.ast_to_lookml_converter import (
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
            ("[Sales]", "${TABLE}.`Sales`"),
            ("[Order Date]", "${TABLE}.`Order Date`"),
            ("[Customer Name]", "${TABLE}.`Customer Name`"),
            ("[Product ID]", "${TABLE}.`Product ID`"),
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
            ("SUM([Sales])", "SUM(${TABLE}.`Sales`)"),
            ("COUNT([Orders])", "COUNT(${TABLE}.`Orders`)"),
            ("AVG([Profit])", "AVG(${TABLE}.`Profit`)"),
            ("MIN([Date])", "MIN(${TABLE}.`Date`)"),
            ("MAX([Price])", "MAX(${TABLE}.`Price`)"),
            ("MEDIAN([Revenue])", "MEDIAN(${TABLE}.`Revenue`)"),
            # New functions from Excel mapping
            ("COUNTD([Customer ID])", "COUNT(DISTINCT ${TABLE}.`Customer ID`)"),
            ("STDEV([Sales])", "STDDEV_SAMP(${TABLE}.`Sales`)"),
            ("STDEVP([Sales])", "STDDEV_POP(${TABLE}.`Sales`)"),
            ("VAR([Profit])", "VAR_SAMP(${TABLE}.`Profit`)"),
            ("VARP([Profit])", "VAR_POP(${TABLE}.`Profit`)"),
            ("CORR([Sales], [Profit])", "CORR(${TABLE}.`Sales`, ${TABLE}.`Profit`)"),
            (
                "COVAR([Sales], [Profit])",
                "COVAR_SAMP(${TABLE}.`Sales`, ${TABLE}.`Profit`)",
            ),
            (
                "COVARP([Sales], [Profit])",
                "COVAR_POP(${TABLE}.`Sales`, ${TABLE}.`Profit`)",
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
    # STRING FUNCTIONS
    # ============================================================================

    def test_string_functions(self, formula_parser, ast_converter):
        """Test string function conversion."""
        test_cases = [
            ("UPPER([Name])", "UPPER(${TABLE}.`Name`)"),
            ("LOWER([Category])", "LOWER(${TABLE}.`Category`)"),
            ("LEN([Title])", "LENGTH(${TABLE}.`Title`)"),
            ("TRIM([Description])", "TRIM(${TABLE}.`Description`)"),
            ("LEFT([Code], 3)", "LEFT(${TABLE}.`Code`, 3)"),
            ("RIGHT([Code], 2)", "RIGHT(${TABLE}.`Code`, 2)"),
            ("MID([Text], 2, 5)", "SUBSTR(${TABLE}.`Text`, 2, 5)"),
            # Advanced string functions
            ("CONTAINS([Name], 'John')", "STRPOS(${TABLE}.`Name`, 'John') > 0"),
            ("STARTSWITH([Code], 'A')", "STARTS_WITH(${TABLE}.`Code`, 'A')"),
            (
                "ENDSWITH([File], '.pdf')",
                "ENDS_WITH(${TABLE}.`File`, '.pdf')",
            ),
            ("REPLACE([Text], 'old', 'new')", "REPLACE(${TABLE}.`Text`, 'old', 'new')"),
            ("FIND([Text], 'word')", "STRPOS(${TABLE}.`Text`, 'word')"),
            ("LTRIM([Text])", "LTRIM(${TABLE}.`Text`)"),
            ("RTRIM([Text])", "RTRIM(${TABLE}.`Text`)"),
            # New functions from Excel mapping
            ("ASCII([Char])", "ASCII(${TABLE}.`Char`)"),
            ("CHAR(65)", "CHR(65)"),
            ("PROPER([Name])", "INITCAP(${TABLE}.`Name`)"),
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
            ("ABS([Profit])", "ABS(${TABLE}.`Profit`)"),
            ("ROUND([Sales], 2)", "ROUND(${TABLE}.`Sales`, 2)"),
            ("CEILING([Price])", "CEIL(${TABLE}.`Price`)"),
            ("FLOOR([Amount])", "FLOOR(${TABLE}.`Amount`)"),
            ("SQRT([Value])", "SQRT(${TABLE}.`Value`)"),
            ("POWER([Base], 2)", "POW(${TABLE}.`Base`, 2)"),
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
            # DATEPART
            (
                "DATEPART('year', [Order Date])",
                "EXTRACT(YEAR FROM ${TABLE}.`Order Date`)",
            ),
            (
                "DATEPART('month', [Order Date])",
                "EXTRACT(MONTH FROM ${TABLE}.`Order Date`)",
            ),
            (
                "DATEPART('day', [Order Date])",
                "EXTRACT(DAY FROM ${TABLE}.`Order Date`)",
            ),
            (
                "DATEPART('week', [Order Date])",
                "EXTRACT(WEEK FROM ${TABLE}.`Order Date`)",
            ),
            (
                "DATEPART('quarter', [Order Date])",
                "EXTRACT(QUARTER FROM ${TABLE}.`Order Date`)",
            ),
            (
                "DATEPART('hour', [Order Date])",
                "EXTRACT(HOUR FROM ${TABLE}.`Order Date`)",
            ),
            (
                "DATEPART('minute', [Order Date])",
                "EXTRACT(MINUTE FROM ${TABLE}.`Order Date`)",
            ),
            (
                "DATEPART('second', [Order Date])",
                "EXTRACT(SECOND FROM ${TABLE}.`Order Date`)",
            ),
            # DATETRUNC
            (
                "DATETRUNC('year', [Order Date])",
                "DATE_TRUNC(${TABLE}.`Order Date`, year)",
            ),
            (
                "DATETRUNC('month', [Order Date])",
                "DATE_TRUNC(${TABLE}.`Order Date`, month)",
            ),
            (
                "DATETRUNC('day', [Order Date])",
                "DATE_TRUNC(${TABLE}.`Order Date`, day)",
            ),
            (
                "DATETRUNC('week', [Order Date])",
                "DATE_TRUNC(${TABLE}.`Order Date`, week)",
            ),
            (
                "DATETRUNC('quarter', [Order Date])",
                "DATE_TRUNC(${TABLE}.`Order Date`, quarter)",
            ),
            (
                "DATETRUNC('hour', [Order Date])",
                "DATE_TRUNC(${TABLE}.`Order Date`, hour)",
            ),
            (
                "DATETRUNC('minute', [Order Date])",
                "DATE_TRUNC(${TABLE}.`Order Date`, minute)",
            ),
            (
                "DATETRUNC('second', [Order Date])",
                "DATE_TRUNC(${TABLE}.`Order Date`, second)",
            ),
            # DATEDIFF
            (
                "DATEDIFF('day', [Order Date], [Ship Date])",
                "DATE_DIFF(${TABLE}.`Ship Date`, ${TABLE}.`Order Date`, DAY)",
            ),
            (
                "DATEDIFF('month', [Order Date], [Ship Date])",
                "DATE_DIFF(${TABLE}.`Ship Date`, ${TABLE}.`Order Date`, MONTH)",
            ),
            (
                "DATEDIFF('year', [Order Date], [Ship Date])",
                "DATE_DIFF(${TABLE}.`Ship Date`, ${TABLE}.`Order Date`, YEAR)",
            ),
            (
                "DATEDIFF('week', [Order Date], [Ship Date])",
                "DATE_DIFF(${TABLE}.`Ship Date`, ${TABLE}.`Order Date`, WEEK)",
            ),
            (
                "DATEDIFF('hour', [Order_Date], [Ship_Date])",
                "DATETIME_DIFF(${TABLE}.`Ship_Date`, ${TABLE}.`Order_Date`, HOUR)",
            ),
            (
                "DATEDIFF('minute', [Order_Date], [Ship_Date])",
                "DATETIME_DIFF(${TABLE}.`Ship_Date`, ${TABLE}.`Order_Date`, MINUTE)",
            ),
            (
                "DATEDIFF('second', [Order_Date], [Ship_Date])",
                "DATETIME_DIFF(${TABLE}.`Ship_Date`, ${TABLE}.`Order_Date`, SECOND)",
            ),
            # DATEADD
            (
                "DATEADD('day', 7, [Order Date])",
                "DATE_ADD(${TABLE}.`Order Date`, INTERVAL 7 DAY)",
            ),
            (
                "DATEADD('month', 1, [Order Date])",
                "DATE_ADD(${TABLE}.`Order Date`, INTERVAL 1 MONTH)",
            ),
            (
                "DATEADD('year', 1, [Order Date])",
                "DATE_ADD(${TABLE}.`Order Date`, INTERVAL 1 YEAR)",
            ),
            (
                "DATEADD('hour', 1, [Order_Date])",
                "DATETIME_ADD(${TABLE}.`Order_Date`, INTERVAL 1 HOUR)",
            ),
            (
                "DATEADD('minute', 1, [Order_Date])",
                "DATETIME_ADD(${TABLE}.`Order_Date`, INTERVAL 1 MINUTE)",
            ),
            (
                "DATEADD('second', 1, [Order_Date])",
                "DATETIME_ADD(${TABLE}.`Order_Date`, INTERVAL 1 SECOND)",
            ),
            # DAY, MONTH, YEAR, WEEK, QUARTER
            ("DAY([Order Date])", "EXTRACT(DAY FROM ${TABLE}.`Order Date`)"),
            ("MONTH([Order Date])", "EXTRACT(MONTH FROM ${TABLE}.`Order Date`)"),
            ("YEAR([Order Date])", "EXTRACT(YEAR FROM ${TABLE}.`Order Date`)"),
            ("WEEK([Order Date])", "EXTRACT(WEEK FROM ${TABLE}.`Order Date`)"),
            ("QUARTER([Order Date])", "EXTRACT(QUARTER FROM ${TABLE}.`Order Date`)"),
            # TODAY and NOW
            ("TODAY()", "CURRENT_DATE()"),
            ("NOW()", "CURRENT_TIMESTAMP()"),
        ]

        for tableau_formula, expected_lookml in test_cases:
            if "DATEADD" in tableau_formula:
                print("yes")
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
            ("FLOAT([Text Value])", "CAST(${TABLE}.`Text Value` AS FLOAT64)"),
            ("INT([Decimal Value])", "CAST(${TABLE}.`Decimal Value` AS INT64)"),
            ("STR([Number])", "CAST(${TABLE}.`Number` AS STRING)"),
            ("DATE([Text Date])", "TIMESTAMP(DATE(${TABLE}.`Text Date`))"),
            ("DATETIME([Text DateTime])", "DATETIME(${TABLE}.`Text DateTime`)"),
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
            ("IFNULL([Value], 0)", "IFNULL(${TABLE}.`Value`, 0)"),
            ("ISNULL([Field])", "${TABLE}.`Field` IS NULL"),
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
            ("[Sales] + [Profit]", "(${TABLE}.`Sales` + ${TABLE}.`Profit`)"),
            ("[Revenue] - [Cost]", "(${TABLE}.`Revenue` - ${TABLE}.`Cost`)"),
            ("[Price] * [Quantity]", "(${TABLE}.`Price` * ${TABLE}.`Quantity`)"),
            ("[Total] / [Count]", "(${TABLE}.`Total` / NULLIF(${TABLE}.`Count`, 0))"),
            ("[Base] ^ 2", "POW(${TABLE}.`Base`, 2)"),
            ("[Value] % 10", "MOD(${TABLE}.`Value`, 10)"),
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
            ("[Sales] > 1000", "(${TABLE}.`Sales` > 1000)"),
            ("[Profit] < 0", "(${TABLE}.`Profit` < 0)"),
            ("[Status] = 'Active'", "(${TABLE}.`Status` = 'Active')"),
            ("[Value] != NULL", "(${TABLE}.`Value` IS NOT NULL)"),
            ("[Score] >= 80", "(${TABLE}.`Score` >= 80)"),
            ("[Rating] <= 5", "(${TABLE}.`Rating` <= 5)"),
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
                "((${TABLE}.`Sales` > 1000) AND (${TABLE}.`Profit` > 0))",
            ),
            (
                "[Status] = 'A' OR [Status] = 'B'",
                "((${TABLE}.`Status` = 'A') OR (${TABLE}.`Status` = 'B'))",
            ),
            ("NOT [Active]", "NOT ${TABLE}.`Active`"),
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
                "CASE WHEN (${TABLE}.`Sales` > 1000) THEN 'High' ELSE 'Low' END",
            ),
            (
                "IF [Profit] > 0 THEN 'Profitable' ELSE 'Loss' END",
                "CASE WHEN (${TABLE}.`Profit` > 0) THEN 'Profitable' ELSE 'Loss' END",
            ),
            (
                "IF [Category] = 'A' THEN 1 ELSE 0 END",
                "CASE WHEN (${TABLE}.`Category` = 'A') THEN 1 ELSE 0 END",
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
            ("UPPER(LEFT([Name], 5))", "UPPER(LEFT(${TABLE}.`Name`, 5))"),
            # Function with arithmetic
            (
                "SUM([Sales] * [Quantity])",
                "SUM((${TABLE}.`Sales` * ${TABLE}.`Quantity`))",
            ),
            # Complex conditional with functions
            (
                "IF LEN([Code]) > 5 THEN UPPER([Code]) ELSE LOWER([Code]) END",
                "CASE WHEN (LENGTH(${TABLE}.`Code`) > 5) THEN UPPER(${TABLE}.`Code`) ELSE LOWER(${TABLE}.`Code`) END",
            ),
            # Multiple operations
            (
                "ROUND([Sales] / [Quantity], 2)",
                "ROUND((${TABLE}.`Sales` / NULLIF(${TABLE}.`Quantity`, 0)), 2)",
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
                assert "${TABLE}" in result or result in [
                    "NULL",
                    "''",
                    "'MIGRATION_REQUIRED'",
                ], f"Unexpected result for {invalid_formula}: {result}"


# ============================================================================
# INTEGRATION TESTS WITH REAL TABLEAU FORMULAS
# ============================================================================


# class TestRealTableauFormulas:
#     """Test with real-world Tableau formulas from sample files."""

#     @pytest.fixture
#     def formula_parser(self):
#         return FormulaParser()

#     @pytest.fixture
#     def ast_converter(self):
#         return ASTToLookMLConverter()

#     def convert_formula(self, tableau_formula: str, formula_parser, ast_converter):
#         """Helper method to convert Tableau formula to LookML."""
#         try:
#             parse_result = formula_parser.parse_formula(tableau_formula)
#             if not parse_result.success:
#                 return f"PARSE_ERROR: {parse_result.error_message}"

#             # Get AST from calculated field
#             if (
#                 not parse_result.calculated_field
#                 or not parse_result.calculated_field.ast_root
#             ):
#                 return "PARSE_ERROR: No AST generated"

#             ast = parse_result.calculated_field.ast_root

#             lookml_sql = ast_converter.convert_to_lookml(ast, "TABLE")
#             return lookml_sql
#         except Exception as e:
#             return f"CONVERSION_ERROR: {str(e)}"

#     def test_sample_calculated_fields(self, formula_parser, ast_converter):
#         """Test with calculated fields from sample Tableau files."""
#         # These are based on actual calculated fields from the sample TWB files
#         real_formulas = [
#             # Basic calculations
#             ("SUM([Sales])", "SUM(${TABLE}.sales)"),
#             ("AVG([Profit Ratio])", "AVG(${TABLE}.profit_ratio)"),
#             # String manipulations
#             ("UPPER([Category])", "UPPER(${TABLE}.category)"),
#             ("LEFT([Product Name], 10)", "LEFT(${TABLE}.product_name, 10)"),
#             # Conditionals from real usage
#             (
#                 "IF [Profit] > 0 THEN 'Profitable' ELSE 'Loss' END",
#                 "CASE WHEN (${TABLE}.profit > 0) THEN 'Profitable' ELSE 'Loss' END",
#             ),
#             # Complex calculations
#             (
#                 "ROUND([Sales] / [Quantity], 2)",
#                 "ROUND((${TABLE}.sales / ${TABLE}.quantity), 2)",
#             ),
#             # Date calculations
#             ("YEAR([Order Date])", "EXTRACT(YEAR FROM ${TABLE}.order_date)"),
#             (
#                 "DATEDIFF([Ship Date], [Order Date], 'day')",
#                 "DATE_DIFF(${TABLE}.order_date, ${TABLE}.ship_date, day)",
#             ),
#         ]

#         for tableau_formula, expected_lookml in real_formulas:
#             result = self.convert_formula(
#                 tableau_formula, formula_parser, ast_converter
#             )
#             assert result == expected_lookml, (
#                 f"Failed for real formula {tableau_formula}: got {result}"
#             )


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "--tb=short"])
