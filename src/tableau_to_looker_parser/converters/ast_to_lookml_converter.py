"""
AST to LookML Converter - Converts Tableau formula AST to LookML SQL expressions.

This module handles the conversion of parsed Tableau formula ASTs into valid LookML SQL syntax.
The converter uses the Visitor pattern to recursively walk the AST tree and build LookML expressions.
"""

import logging
from typing import Dict
from ..models.ast_schema import ASTNode, NodeType, DataType
from typing import Optional
import re

logger = logging.getLogger(__name__)


class ASTToLookMLConverter:
    """
    Converts Tableau formula AST nodes to LookML SQL expressions.

    Key Design Principles:
    1. Recursive tree traversal using the Visitor pattern
    2. Each node type has a specific conversion method
    3. Simple string building - no complex logic
    4. Function registry for Tableau → LookML function mapping
    """

    def __init__(self):
        """Initialize the converter with function mappings."""
        self.function_registry = self._build_function_registry()
        logger.debug("AST to LookML converter initialized")

    def convert_to_lookml(
        self,
        ast_node: ASTNode,
        table_context: str = "TABLE",
        table_name: Optional[str] = None,
    ) -> str:
        """
        Convert an AST node to LookML SQL expression.

        Args:
            ast_node: Root AST node to convert
            table_context: Table context for field references (default: "TABLE")

        Returns:
            str: LookML SQL expression

        Example:
            Input AST: FieldRef(field_name="adult")
            Output: "${TABLE}.adult"
        """
        try:
            result = self._convert_node(ast_node, table_context, table_name)
            logger.debug(f"Converted AST to LookML: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to convert AST to LookML: {str(e)}")
            return f"/* Conversion error: {str(e)} */"

    def _convert_node(
        self, node: ASTNode, table_context: str, table_name: Optional[str] = None
    ) -> str:
        """
        Core recursive method that converts individual AST nodes.

        This is the heart of the converter - it dispatches to specific
        conversion methods based on the node type.
        """
        if node is None:
            return "NULL"

        # Dispatch to specific conversion methods based on node type
        # This is the Visitor pattern in action
        if node.node_type == NodeType.FIELD_REF:
            return self._convert_field_reference(node, table_context)
        elif node.node_type == NodeType.LITERAL:
            return self._convert_literal(node)
        elif node.node_type == NodeType.ARITHMETIC:
            return self._convert_arithmetic(node, table_context)
        elif node.node_type == NodeType.COMPARISON:
            return self._convert_comparison(node, table_context)
        elif node.node_type == NodeType.LOGICAL:
            return self._convert_logical(node, table_context)
        elif node.node_type == NodeType.FUNCTION:
            return self._convert_function(node, table_context)
        elif node.node_type == NodeType.CONDITIONAL:
            return self._convert_conditional(node, table_context)
        elif node.node_type == NodeType.CASE:
            return self._convert_case(node, table_context)
        elif node.node_type == NodeType.PARAMETER_REF:
            return self._convert_parameter_ref(node, table_context)
        elif node.node_type == NodeType.LOD_EXPRESSION:
            return self._convert_lod_expression(node, table_context)
        elif node.node_type == NodeType.DERIVED_TABLE:
            return self._convert_derived_table(node, table_context, table_name)
        elif node.node_type == NodeType.WINDOW_FUNCTION:
            return self._convert_window_function(node, table_context)
        elif node.node_type == NodeType.UNARY:
            return self._convert_unary(node, table_context)
        else:
            # Check if this is a fallback node with migration metadata
            if (
                node.node_type == NodeType.LITERAL
                and node.properties
                and node.properties.get("migration_status") == "MANUAL_REQUIRED"
            ):
                return self._convert_fallback_node(node, table_context)

            logger.warning(f"Unsupported node type: {node.node_type}")
            return f"/* Unsupported: {node.node_type} */"

    def _build_function_registry(self) -> Dict[str, str]:
        """
        Build the function mapping registry.

        This maps Tableau function names to their LookML equivalents.
        Most are direct mappings, but some require transformation.
        """
        return {
            # Aggregation functions - direct mapping
            "SUM": "SUM",
            "COUNT": "COUNT",
            "AVG": "AVG",
            "MIN": "MIN",
            "MAX": "MAX",
            "MEDIAN": "MEDIAN",
            # Additional aggregation functions from Excel mapping
            "COUNTD": "COUNT(DISTINCT {0})",  # COUNT DISTINCT
            "STDEV": "STDDEV_SAMP",  # Standard deviation (sample)
            "STDEVP": "STDDEV_POP",  # Standard deviation (population)
            "CORR": "CORR",  # Correlation
            "COVAR": "COVAR_SAMP",  # Covariance (sample)
            "COVARP": "COVAR_POP",  # Covariance (population)
            "VAR": "VAR_SAMP",  # Variance (sample)
            "VARP": "VAR_POP",  # Variance (population)
            # String functions - mostly direct
            "UPPER": "UPPER",
            "LOWER": "LOWER",
            "LEN": "LENGTH",  # Tableau LEN → SQL LENGTH
            "TRIM": "TRIM",
            "LEFT": "LEFT",
            "RIGHT": "RIGHT",
            "MID": "SUBSTR",  # Tableau MID → SQL SUBSTR
            # Advanced string functions
            "CONTAINS": "STRPOS({0}, {1}) > 0",  # CONTAINS(string, substring) → STRPOS(string, substring) > 0 for BigQuery
            "STARTSWITH": "STARTS_WITH({0}, {1})",  # STARTSWITH(string, prefix) → STARTS_WITH(string, prefix) for BigQuery
            "ENDSWITH": "ENDS_WITH({0}, {1})",  # ENDSWITH(string, suffix) → ENDS_WITH(string, suffix) for BigQuery
            "REPLACE": "REPLACE",  # Direct mapping
            "FIND": "FIND_SPECIAL",  # FIND(string, substring) → STRPOS(string, substring) for BigQuery
            "SPLIT": "SPLIT({0}, {1})[OFFSET(CASE WHEN {2} < 0 THEN ARRAY_LENGTH(SPLIT({0}, {1})) + {2} ELSE {2} - 1 END)]",  # SPLIT(string, delimiter, index) → SPLIT(string, delimiter)[OFFSET(...)] for BigQuery
            "LTRIM": "LTRIM",  # Direct mapping
            "RTRIM": "RTRIM",  # Direct mapping
            # Additional string functions from Excel mapping
            "ASCII": "ASCII",  # ASCII code of character
            "CHAR": "CHR",  # Character from ASCII code (Tableau CHAR → SQL CHR)
            "PROPER": "INITCAP",  # Proper case (Tableau PROPER → SQL INITCAP)
            "SPACE": "REPEAT('\\u2009', {0})",  # SPACE(n) → REPEAT(' ', n)
            # Math functions - direct mapping
            "ABS": "ABS",
            "ROUND": "ROUND",
            "CEILING": "CEIL",  # Tableau CEILING → SQL CEIL
            "FLOOR": "FLOOR",
            "SQRT": "SQRT",
            "POWER": "POW",
            "DIV": "DIV",
            "SQUARE": "POW({0}, 2)",  # SQUARE(x) → POWER(x, 2)
            "PI": "ACOS(-1)",  # PI() → ACOS(-1) for BigQuery
            "SIGN": "SIGN",
            "SIN": "SIN",
            "COS": "COS",
            "TAN": "TAN",
            "COT": "1 /NULLIF(TAN({0}),0)",  # COT(x) → 1 / TAN(x)
            "ASIN": "ASIN",
            "ACOS": "ACOS",
            "ATAN": "ATAN",
            "LOG": "LOG",
            "LN": "LN",
            # Date functions - more complex, handled specially
            "YEAR": "EXTRACT(YEAR FROM {})",
            "MONTH": "EXTRACT(MONTH FROM {})",
            "DAY": "EXTRACT(DAY FROM {})",
            "WEEK": "EXTRACT(WEEK FROM {})",
            "QUARTER": "EXTRACT(QUARTER FROM {})",
            "NOW": "CURRENT_TIMESTAMP",
            "TODAY": "CURRENT_DATE",
            # Additional date functions from Excel mapping - need special handling
            "DATEADD": "DATEADD_SPECIAL",  # Special handling for INTERVAL syntax
            "DATEDIFF": "DATEDIFF_SPECIAL",  # Special handling for unit parameter
            "DATETRUNC": "DATETRUNC_SPECIAL",  # Special handling for unit parameter
            "DATEPART": "DATEPART_SPECIAL",
            "PARSE_DATE": "PARSE_DATE('%Y-%m-%d', {0})",  # PARSE_DATE format
            # Type conversion functions from Excel mapping
            "FLOAT": "CAST({0} AS FLOAT64)",  # Convert to float
            "INT": "CAST({0} AS INT64)",  # Convert to integer
            "STR": "CAST({0} AS STRING)",  # Convert to string
            "DATE": "DATE({0})",  # Convert to date
            "DATETIME": "DATETIME({0})",  # Convert to datetime
            # Logical functions from Excel mapping
            "IFNULL": "IFNULL",  # NULL handling function
            "ISNULL": "{0} IS NULL",  # NULL check: ISNULL([field]) -> field IS NULL
            "ZN": "IFNULL({0}, 0)",  # Zero if null: ZN([field]) -> IFNULL(field, 0)
            "MAKEPOINT": "MAKEPOINT_SPECIAL",
            "MAKELINE": "MAKELINE_SPECIAL",
        }

    # CONVERSION METHODS - Each handles a specific AST node type

    def _convert_field_reference(self, node: ASTNode, table_context: str) -> str:
        """
        Convert field reference: [field_name] → ${TABLE}.field_name

        This is the SIMPLEST conversion - just wrap field name in LookML syntax.

        Args:
            node: FieldRef AST node with field_name attribute
            table_context: Table context (usually "TABLE")

        Returns:
            LookML field reference

        Examples:
            [adult] → ${TABLE}.adult
            [budget] → ${TABLE}.budget
            [Movie Title] → ${TABLE}.movie_title (spaces converted to underscores)
        """
        if not node.field_name:
            logger.warning("Field reference node missing field_name")
            return "/* Missing field name */"

        # Clean field name - replace spaces with underscores, make lowercase
        clean_field_name = node.field_name
        clean_field_name = re.sub(
            r"\[([^\]]+)\]", r"\1", clean_field_name
        )  # Remove brackets
        clean_field_name = re.sub(
            r"[^\w\s]", "_", clean_field_name
        )  # Replace special chars with underscore
        clean_field_name = re.sub(
            r"\s+", "_", clean_field_name
        )  # Replace spaces with underscore
        clean_field_name = re.sub(
            r"_+", "_", clean_field_name
        )  # Replace multiple underscores with single
        clean_field_name = clean_field_name.strip("_").lower()

        # Build LookML field reference
        numbers_after_underscore = re.findall(r"_(\d+)", clean_field_name)

        # Flatten all digits into one string
        total_digits = "".join(numbers_after_underscore)

        if len(total_digits) >= 10 or clean_field_name == "max_dttm":
            # Treat it as a global field reference
            lookml_ref = f"${{{clean_field_name}}}"
        else:
            # Default case with table prefix
            lookml_ref = f"${{{table_context}}}.{clean_field_name}"

        logger.debug(f"Converted field reference: {node.field_name} → {lookml_ref}")
        return lookml_ref

    def _convert_literal(self, node: ASTNode) -> str:
        """
        Convert literal values: strings, numbers, booleans, null.

        This handles constants in formulas like "Adult", 123, TRUE, NULL.

        Examples:
            "Hello" → 'Hello'  (wrap strings in quotes)
            123 → 123          (numbers as-is)
            TRUE → TRUE        (booleans as-is)
            NULL → NULL        (null as-is)
        """
        if node.value is None:
            return "NULL"

        # Handle different data types
        if node.data_type == DataType.STRING:
            # Escape single quotes and wrap in quotes
            escaped_value = str(node.value).replace("'", "\\'")
            return f"'{escaped_value}'"

        elif node.data_type == DataType.BOOLEAN:
            # Convert Python boolean to SQL boolean
            return "TRUE" if node.value else "FALSE"

        elif node.data_type in [DataType.INTEGER, DataType.REAL]:
            # Numbers can be used directly
            return str(node.value)

        else:
            # Default: treat as string
            escaped_value = str(node.value).replace("'", "\\'")
            return f"'{escaped_value}'"

    def _convert_arithmetic(self, node: ASTNode, table_context: str) -> str:
        """
        Convert arithmetic operations: +, -, *, /, %, ^

        This is where RECURSION happens! We convert the left and right
        child nodes, then combine them with the operator.

        Args:
            node: Arithmetic AST node with operator, left, right
            table_context: Table context for child nodes

        Returns:
            LookML arithmetic expression

        Examples:
            [budget] + [revenue] → (${TABLE}.budget + ${TABLE}.revenue)
            [popularity] * 2     → (${TABLE}.popularity * 2)
            [budget] / [runtime] → (${TABLE}.budget / ${TABLE}.runtime)
        """
        if not node.left or not node.right:
            logger.warning("Arithmetic node missing left or right operand")
            return "/* Missing operand */"

        # RECURSION: Convert left and right child nodes
        left_expr = self._convert_node(node.left, table_context)
        right_expr = self._convert_node(node.right, table_context)

        # Handle special operators
        operator = node.operator
        if operator == "^":
            # Tableau uses ^ for power, SQL uses POW function
            return f"POW({left_expr}, {right_expr})"
        elif operator == "%":
            # Modulo operator
            return f"MOD({left_expr}, {right_expr})"
        else:
            # Standard operators: +, -, *, /
            # Wrap in parentheses to preserve precedence
            return f"({left_expr} {operator} {right_expr})"

    def _convert_comparison(self, node: ASTNode, table_context: str) -> str:
        """
        Convert comparison operations: =, !=, <, >, <=, >=

        Similar to arithmetic, but for comparison operators.

        Examples:
            [adult] = TRUE        → (${TABLE}.adult = TRUE)
            [budget] > 1000000    → (${TABLE}.budget > 1000000)
            [rating] <= 5.0       → (${TABLE}.rating <= 5.0)
        """
        if not node.left or not node.right:
            logger.warning("Comparison node missing left or right operand")
            return "/* Missing operand */"

        # RECURSION: Convert both sides
        left_expr = self._convert_node(node.left, table_context)
        right_expr = self._convert_node(node.right, table_context)

        # Handle special comparison operators
        operator = node.operator
        if operator == "<>" or operator == "!=":
            # Both Tableau <> and != map to SQL !=
            operator = "!="

        return f"({left_expr} {operator} {right_expr})"

    def _convert_logical(self, node: ASTNode, table_context: str) -> str:
        """
        Convert logical operations: AND, OR

        Examples:
            [adult] AND [rated_r] → (${TABLE}.adult AND ${TABLE}.rated_r)
            [budget] > 1000 OR [revenue] > 5000 → (...complex expression...)
        """
        if not node.left or not node.right:
            logger.warning("Logical node missing left or right operand")
            return "/* Missing operand */"

        # RECURSION: Convert both sides
        left_expr = self._convert_node(node.left, table_context)
        right_expr = self._convert_node(node.right, table_context)

        operator = node.operator.upper()  # Ensure uppercase AND, OR
        return f"({left_expr} {operator} {right_expr})"

    def _convert_unary(self, node: ASTNode, table_context: str) -> str:
        """
        Convert unary operations: NOT, - (negative)

        Examples:
            NOT [adult]  → NOT ${TABLE}.adult
            -[budget]    → -${TABLE}.budget
        """
        if not node.operand:
            logger.warning("Unary node missing operand")
            return "/* Missing operand */"

        # RECURSION: Convert the operand
        operand_expr = self._convert_node(node.operand, table_context)

        operator = node.operator.upper() if node.operator else ""

        if operator == "NOT":
            return f"NOT {operand_expr}"
        elif operator == "-":
            return f"-{operand_expr}"
        else:
            return f"{operator}{operand_expr}"

    def _convert_function(self, node: ASTNode, table_context: str) -> str:
        """
        Convert function calls using the function registry.

        This is the SMART part - we map Tableau functions to LookML equivalents.

        Args:
            node: Function AST node with function_name and arguments
            table_context: Table context for arguments

        Returns:
            LookML function call

        Examples:
            UPPER([title]) → UPPER(${TABLE}.title)
            SUM([budget])  → SUM(${TABLE}.budget)
            LEN([title])   → LENGTH(${TABLE}.title)    # Function name mapping!
        """
        if not node.function_name:
            logger.warning("Function node missing function_name")
            return "/* Missing function name */"

        function_name = node.function_name.upper()

        # Convert all arguments recursively
        converted_args = []
        for arg in node.arguments:
            arg_expr = self._convert_node(arg, table_context)
            converted_args.append(arg_expr)

        # Look up function in registry for other functions
        if function_name in self.function_registry:
            lookml_function = self.function_registry[function_name]

            # Handle special date functions that need custom formatting
            if lookml_function == "DATEADD_SPECIAL":
                if len(converted_args) == 3:
                    date_expr = converted_args[0]
                    date_expr = date_expr.strip("'\"").upper()
                    interval_expr = converted_args[1]
                    unit_expr = converted_args[2].strip(
                        "'\""
                    )  # Remove quotes from unit
                    if (
                        date_expr == "HOUR"
                        or date_expr == "MINUTE"
                        or date_expr == "SECOND"
                    ):
                        return f"DATETIME_ADD({unit_expr}, INTERVAL {interval_expr} {date_expr})"
                    else:
                        return f"DATE_ADD({unit_expr}, INTERVAL {interval_expr} {date_expr})"
                else:
                    return (
                        f"/* DATEADD: expects 3 arguments, got {len(converted_args)} */"
                    )
            elif lookml_function == "DATEDIFF_SPECIAL":
                if len(converted_args) == 3:
                    date_expr = converted_args[0]
                    date_expr = date_expr.strip("'\"").upper()
                    start_expr = converted_args[1].strip("'\"")
                    end_expr = converted_args[2].strip("'\"")  # Remove quotes from unit
                    if (
                        date_expr == "HOUR"
                        or date_expr == "MINUTE"
                        or date_expr == "SECOND"
                    ):
                        return f"DATETIME_DIFF({end_expr}, {start_expr}, {date_expr})"
                    else:
                        return f"DATE_DIFF({end_expr}, {start_expr}, {date_expr})"
                else:
                    return f"/* DATEDIFF: expects 3 arguments, got {len(converted_args)} */"
            elif lookml_function == "DATETRUNC_SPECIAL":
                if len(converted_args) == 2:
                    date_expr = converted_args[0]
                    date_expr = date_expr.strip("'\"")
                    unit_expr = converted_args[1].strip(
                        "'\""
                    )  # Remove quotes from unit
                    return f"DATE_TRUNC({unit_expr}, {date_expr})"
                else:
                    return f"/* DATETRUNC: expects 2 arguments, got {len(converted_args)} */"
            elif lookml_function == "DATEPART_SPECIAL":
                if len(converted_args) == 2:
                    date_expr = converted_args[0]
                    date_expr = date_expr.strip("'\"").upper()
                    unit_expr = converted_args[1].strip("'\"")
                    return f"EXTRACT({date_expr} FROM {unit_expr})"
                else:
                    return f"/* DATEPART: expects 2 arguments, got {len(converted_args)} */"
            elif lookml_function == "FIND_SPECIAL":
                if len(converted_args) == 3:
                    return f"STRPOS(SUBSTR({converted_args[0]}, {converted_args[2]}), {converted_args[1]}) + {converted_args[2]} - 1"
                elif len(converted_args) == 2:
                    return f"STRPOS({converted_args[0]}, {converted_args[1]})"
                else:
                    return f"/* FIND: expects 2 or 3 arguments, got {len(converted_args)} */"
            elif lookml_function == "LOG":
                if len(converted_args) == 1:
                    # LOG(value) - use base 10 as default
                    return f"LOG({converted_args[0]},10)"  # LOG base 10
                elif len(converted_args) == 2:
                    # LOG(value, base) - use specified base
                    return f"LOG({converted_args[0]}, {converted_args[1]})"
                else:
                    return f"/* LOG: expects 1 or 2 arguments, got {len(converted_args)} */"
            elif lookml_function == "MAKEPOINT_SPECIAL":
                if len(converted_args) == 2:
                    lat_expr = converted_args[0]
                    lng_expr = converted_args[1]
                    return f"ST_GEOGPOINT({lng_expr}, {lat_expr})"
                else:
                    return f"/* MAKEPOINT expects 2 arguments, got {len(converted_args)} */"

            elif lookml_function == "MAKELINE_SPECIAL":
                if len(converted_args) == 2:
                    return f"ST_MAKELINE({converted_args[0]}, {converted_args[1]})"
                else:
                    return (
                        f"/* MAKELINE expects 2 arguments, got {len(converted_args)} */"
                    )

            # Handle special function formats
            elif "{}" in lookml_function:
                # Special format like EXTRACT(YEAR FROM {})
                if len(converted_args) == 1:
                    return lookml_function.format(converted_args[0])
                else:
                    logger.warning(
                        f"Special function {function_name} expects 1 argument, got {len(converted_args)}"
                    )
                    return f"/* {function_name}: wrong argument count */"
            elif "{0}" in lookml_function or "{1}" in lookml_function:
                # Complex string function patterns with numbered placeholders
                try:
                    return lookml_function.format(*converted_args)
                except (IndexError, KeyError) as e:
                    logger.warning(f"Function {function_name} template error: {str(e)}")
                    return f"/* {function_name}: template error */"
            else:
                # Standard function format: FUNCTION(arg1, arg2, ...)
                args_str = ", ".join(converted_args)
                # Handle zero-argument functions that shouldn't have parentheses
                if len(converted_args) == 0 and lookml_function in [
                    "CURRENT_TIMESTAMP",
                    "CURRENT_DATE",
                ]:
                    return f"{lookml_function}()"
                # Handle functions where lookml_function is an expression with parentheses already
                elif "(" in lookml_function and ")" in lookml_function:
                    return lookml_function
                else:
                    return f"{lookml_function}({args_str})"
        else:
            # Function not in registry - use as-is with warning
            logger.warning(f"Unknown function: {function_name}")
            args_str = ", ".join(converted_args)
            return f"{function_name}({args_str})"

    def _convert_conditional(self, node: ASTNode, table_context: str) -> str:
        """
        Convert IF-THEN-ELSE to CASE-WHEN-ELSE.

        This is the MOST COMPLEX conversion because Tableau and LookML
        use different syntax for conditionals.

        Args:
            node: Conditional AST node with condition, then_branch, else_branch
            table_context: Table context for expressions

        Returns:
            LookML CASE expression

        Examples:
            IF [adult] THEN "Adult" ELSE "Child" END
            ↓
            CASE WHEN ${TABLE}.adult THEN 'Adult' ELSE 'Child' END

            IF [budget] > 1000000 THEN "Blockbuster" ELSE "Independent" END
            ↓
            CASE WHEN (${TABLE}.budget > 1000000) THEN 'Blockbuster' ELSE 'Independent' END
        """
        if not node.condition or not node.then_branch:
            logger.warning("Conditional node missing condition or then_branch")
            return "/* Incomplete conditional */"

        # Convert each part recursively
        condition_expr = self._convert_node(node.condition, table_context)
        then_expr = self._convert_node(node.then_branch, table_context)

        # Handle optional ELSE clause
        if node.else_branch:
            else_expr = self._convert_node(node.else_branch, table_context)
        else:
            else_expr = "NULL"

        # Build CASE expression
        case_expr = f"CASE WHEN {condition_expr} THEN {then_expr} ELSE {else_expr} END"

        logger.debug(f"Converted conditional to: {case_expr}")
        return case_expr

    def _convert_case(self, node: ASTNode, table_context: str) -> str:
        """
        Convert CASE-WHEN-ELSE to LookML CASE expression.

        Tableau CASE statements can be:
        1. Simple CASE: CASE [field] WHEN value1 THEN result1 WHEN value2 THEN result2 ELSE default END
        2. Searched CASE: CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 ELSE default END

        Args:
            node: CASE AST node with case_expression, when_clauses, else_branch
            table_context: Table context for expressions

        Returns:
            LookML CASE expression

        Examples:
            CASE [category] WHEN "Electronics" THEN 0.1 WHEN "Books" THEN 0.05 ELSE 0 END
            ↓
            CASE ${TABLE}.category WHEN 'Electronics' THEN 0.1 WHEN 'Books' THEN 0.05 ELSE 0 END

            CASE WHEN [sales] > 1000 THEN "High" WHEN [sales] > 500 THEN "Medium" ELSE "Low" END
            ↓
            CASE WHEN (${TABLE}.sales > 1000) THEN 'High' WHEN (${TABLE}.sales > 500) THEN 'Medium' ELSE 'Low' END
        """
        if not node.when_clauses:
            logger.warning("CASE node missing when_clauses")
            return "/* CASE statement with no WHEN clauses */"

        case_parts = ["CASE"]

        base_case_expr = (
            self._convert_node(node.case_expression, table_context)
            if node.case_expression
            else None
        )

        for when_clause in node.when_clauses:
            raw_condition_expr = self._convert_node(
                when_clause.condition, table_context
            )
            result_expr = self._convert_node(when_clause.result, table_context)
            if base_case_expr is not None:
                if when_clause.condition.node_type in (
                    NodeType.COMPARISON,
                    NodeType.LOGICAL,
                    NodeType.CONDITIONAL,
                ):
                    condition_expr = raw_condition_expr
                else:
                    condition_expr = f"({base_case_expr} = {raw_condition_expr})"
            else:
                condition_expr = raw_condition_expr

            case_parts.append(f"WHEN {condition_expr} THEN {result_expr}")

        # Handle optional ELSE clause
        if node.else_branch:
            else_expr = self._convert_node(node.else_branch, table_context)
            case_parts.append(f"ELSE {else_expr}")

        case_parts.append("END")

        # Build final CASE expression
        case_expr = " ".join(case_parts)

        logger.debug(f"Converted CASE statement to: {case_expr}")
        return case_expr

    def _convert_lod_expression(self, node: ASTNode, table_context: str) -> str:
        """
        Convert LOD expressions to LookML SQL subqueries.

        LOD expressions change the aggregation context:
        - FIXED: Aggregates at specified dimensions only
        - INCLUDE: Adds dimensions to current context
        - EXCLUDE: Removes dimensions from current context

        Args:
            node: LOD AST node with lod_type, lod_dimensions, lod_expression
            table_context: Table context for field references

        Returns:
            LookML SQL subquery expression

        Examples:
            {FIXED [Region] : SUM([Sales])}
            ↓
            (SELECT SUM(sales) FROM ${TABLE} GROUP BY region)

            {INCLUDE [Category] : AVG([Profit])}
            ↓
            (SELECT AVG(profit) FROM ${TABLE} GROUP BY /* current context + */ category)
        """
        if not node.lod_type or not node.lod_expression:
            logger.warning("LOD expression missing type or expression")
            return "/* Invalid LOD expression */"

        # Convert the aggregation expression
        lod_expr = self._convert_node(node.lod_expression, table_context)

        # Convert dimension fields
        dimension_fields = []
        for dim in node.lod_dimensions:
            dim_field = self._convert_node(dim, table_context)
            # Remove ${TABLE}. prefix for GROUP BY
            clean_field = dim_field.replace(f"${{{table_context}}}.", "")
            dimension_fields.append(clean_field)

        # Build SQL based on LOD type
        if node.lod_type == "FIXED":
            # FIXED: Aggregate only at specified dimensions
            if dimension_fields:
                group_by = f" GROUP BY {', '.join(dimension_fields)}"
            else:
                group_by = ""  # No dimensions = grand total

            sql_expr = f"(SELECT {lod_expr} FROM ${{{table_context}}}{group_by})"

        elif node.lod_type == "INCLUDE":
            # INCLUDE: Add dimensions to current context
            # This is complex - for now, treat like FIXED
            if dimension_fields:
                group_by = f" GROUP BY {', '.join(dimension_fields)}"
            else:
                group_by = ""

            sql_expr = f"(SELECT {lod_expr} FROM ${{{table_context}}}{group_by})"

        elif node.lod_type == "EXCLUDE":
            # EXCLUDE: Remove dimensions from current context
            # This is complex - for now, create a simple subquery
            sql_expr = f"(SELECT {lod_expr} FROM ${{{table_context}}})"

        else:
            logger.warning(f"Unsupported LOD type: {node.lod_type}")
            sql_expr = f"/* Unsupported LOD type: {node.lod_type} */"

        logger.debug(f"Converted LOD expression to: {sql_expr}")
        return sql_expr

    def _convert_derived_table(
        self, node: ASTNode, table_context: str, table_name: Optional[str] = None
    ) -> str:
        """
        Convert derived table expressions like {MAX([DTTM])} into LookML SQL subqueries.

        Args:
            node: Derived table AST node with aggregation_function, field_name, etc.
            table_context: Not used here; relies on explicit properties.

        Returns:
            str: LookML-compatible SQL subquery
        """
        props = node.properties or {}

        # Required fields
        agg_func = props.get("aggregation_function")
        field = props.get("field_name")
        table_name = table_name
        derived_alias = props.get("derived_table_alias", "derived_table")
        derived_field_alias = props.get("derived_field_alias", "DerivedValue")
        table_alias = props.get("table_alias", "base")

        if not agg_func or not field or not table_name:
            return f"/* Missing info for derived table: {props} */"

        # Generate subquery: SELECT AGG(field) AS alias FROM table
        subquery = (
            f"(SELECT {agg_func}({field}) AS {derived_field_alias} FROM {table_name})"
        )

        final_sql = (
            f"WITH {derived_alias} AS "
            f"{subquery} "
            f"SELECT {table_alias}.*, {derived_alias}.{derived_field_alias} "
            f"FROM {table_name} AS {table_alias} "
            f"CROSS JOIN {derived_alias}"
        )

        # Final SQL expression (can be wrapped or used in FROM clause)
        return final_sql

    def _convert_window_function(self, node: ASTNode, table_context: str) -> str:
        """
        Convert window functions to LookML SQL with OVER clauses.

        Window functions require OVER clauses to define partitioning and ordering:
        - RUNNING_SUM: Cumulative sum with default ordering
        - RANK: Ranking with optional ordering direction
        - WINDOW_SUM: Window sum with frame specification

        Args:
            node: Window function AST node with window_function_type and arguments
            table_context: Table context for field references

        Returns:
            LookML SQL window function expression

        Examples:
            RUNNING_SUM([Sales]) → SUM(${TABLE}.sales) OVER (ORDER BY ${TABLE}.sales)
            RANK([Sales], 'desc') → RANK() OVER (ORDER BY ${TABLE}.sales DESC)
            WINDOW_SUM([Sales], -2, 0) → SUM(${TABLE}.sales) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        """
        if not node.window_function_type:
            logger.warning("Window function node missing window_function_type")
            return "/* Invalid window function */"

        window_func = node.window_function_type.upper()

        # Convert arguments
        converted_args = []
        for arg in node.arguments:
            arg_expr = self._convert_node(arg, table_context)
            converted_args.append(arg_expr)

        # Build window function based on type
        if window_func == "RUNNING_SUM":
            # RUNNING_SUM([field]) → SUM(field) OVER (ORDER BY field)
            if len(converted_args) == 1:
                field_expr = converted_args[0]
                sql_expr = f"SUM({field_expr}) OVER (ORDER BY {field_expr})"
            else:
                logger.warning(
                    f"RUNNING_SUM expects 1 argument, got {len(converted_args)}"
                )
                sql_expr = "/* RUNNING_SUM: wrong argument count */"

        elif window_func == "RUNNING_AVG":
            # RUNNING_AVG([field]) → AVG(field) OVER (ORDER BY field)
            if len(converted_args) == 1:
                field_expr = converted_args[0]
                sql_expr = f"AVG({field_expr}) OVER (ORDER BY {field_expr})"
            else:
                logger.warning(
                    f"RUNNING_AVG expects 1 argument, got {len(converted_args)}"
                )
                sql_expr = "/* RUNNING_AVG: wrong argument count */"

        elif window_func == "RUNNING_COUNT":
            # RUNNING_COUNT([field]) → COUNT(field) OVER (ORDER BY field)
            if len(converted_args) == 1:
                field_expr = converted_args[0]
                sql_expr = f"COUNT({field_expr}) OVER (ORDER BY {field_expr})"
            else:
                logger.warning(
                    f"RUNNING_COUNT expects 1 argument, got {len(converted_args)}"
                )
                sql_expr = "/* RUNNING_COUNT: wrong argument count */"

        elif window_func == "RANK":
            # RANK([field]) → RANK() OVER (ORDER BY field)
            # RANK([field], 'desc') → RANK() OVER (ORDER BY field DESC)
            if len(converted_args) >= 1:
                field_expr = converted_args[0]
                order_direction = "ASC"

                if len(converted_args) == 2:
                    # Second argument is direction (desc/asc)
                    direction_arg = converted_args[1].strip("'\"").upper()
                    if direction_arg in ["DESC", "DESCENDING"]:
                        order_direction = "DESC"

                sql_expr = f"RANK() OVER (ORDER BY {field_expr} {order_direction})"
            else:
                logger.warning(
                    f"RANK expects at least 1 argument, got {len(converted_args)}"
                )
                sql_expr = "/* RANK: missing arguments */"

        elif window_func == "DENSE_RANK":
            # DENSE_RANK([field]) → DENSE_RANK() OVER (ORDER BY field)
            if len(converted_args) >= 1:
                field_expr = converted_args[0]
                order_direction = "ASC"

                if len(converted_args) == 2:
                    direction_arg = converted_args[1].strip("'\"").upper()
                    if direction_arg in ["DESC", "DESCENDING"]:
                        order_direction = "DESC"

                sql_expr = (
                    f"DENSE_RANK() OVER (ORDER BY {field_expr} {order_direction})"
                )
            else:
                logger.warning(
                    f"DENSE_RANK expects at least 1 argument, got {len(converted_args)}"
                )
                sql_expr = "/* DENSE_RANK: missing arguments */"

        elif window_func == "ROW_NUMBER":
            # ROW_NUMBER() → ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
            sql_expr = "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))"

        elif window_func == "WINDOW_SUM":
            # WINDOW_SUM([field], start, end) → SUM(field) OVER (ROWS BETWEEN ... AND ...)
            if len(converted_args) == 3:
                field_expr = converted_args[0]
                start_offset = converted_args[1]
                end_offset = converted_args[2]

                # Convert offsets to ROWS clause
                start_clause = self._convert_window_offset(start_offset, "PRECEDING")
                end_clause = self._convert_window_offset(end_offset, "FOLLOWING")

                sql_expr = f"SUM({field_expr}) OVER (ROWS BETWEEN {start_clause} AND {end_clause})"
            else:
                logger.warning(
                    f"WINDOW_SUM expects 3 arguments, got {len(converted_args)}"
                )
                sql_expr = "/* WINDOW_SUM: wrong argument count */"

        elif window_func == "WINDOW_AVG":
            # WINDOW_AVG([field], start, end) → AVG(field) OVER (ROWS BETWEEN ... AND ...)
            if len(converted_args) == 3:
                field_expr = converted_args[0]
                start_offset = converted_args[1]
                end_offset = converted_args[2]

                start_clause = self._convert_window_offset(start_offset, "PRECEDING")
                end_clause = self._convert_window_offset(end_offset, "FOLLOWING")

                sql_expr = f"AVG({field_expr}) OVER (ROWS BETWEEN {start_clause} AND {end_clause})"
            else:
                logger.warning(
                    f"WINDOW_AVG expects 3 arguments, got {len(converted_args)}"
                )
                sql_expr = "/* WINDOW_AVG: wrong argument count */"

        elif window_func == "LAG":
            # LAG([field], offset, default) → LAG(field, offset, default) OVER (ORDER BY field)
            if len(converted_args) >= 1:
                field_expr = converted_args[0]
                offset = converted_args[1] if len(converted_args) > 1 else "1"
                default_val = converted_args[2] if len(converted_args) > 2 else "NULL"

                sql_expr = f"LAG({field_expr}, {offset}, {default_val}) OVER (ORDER BY {field_expr})"
            else:
                logger.warning(
                    f"LAG expects at least 1 argument, got {len(converted_args)}"
                )
                sql_expr = "/* LAG: missing arguments */"

        elif window_func == "LEAD":
            # LEAD([field], offset, default) → LEAD(field, offset, default) OVER (ORDER BY field)
            if len(converted_args) >= 1:
                field_expr = converted_args[0]
                offset = converted_args[1] if len(converted_args) > 1 else "1"
                default_val = converted_args[2] if len(converted_args) > 2 else "NULL"

                sql_expr = f"LEAD({field_expr}, {offset}, {default_val}) OVER (ORDER BY {field_expr})"
            else:
                logger.warning(
                    f"LEAD expects at least 1 argument, got {len(converted_args)}"
                )
                sql_expr = "/* LEAD: missing arguments */"

        else:
            # Unknown window function - use generic OVER clause
            logger.warning(f"Unknown window function: {window_func}")
            args_str = ", ".join(converted_args) if converted_args else ""
            sql_expr = f"{window_func}({args_str}) OVER ()"

        logger.debug(f"Converted window function {window_func} to: {sql_expr}")
        return sql_expr

    def _convert_window_offset(self, offset_str: str, direction: str) -> str:
        """
        Convert window function offset to SQL ROWS clause.

        Args:
            offset_str: Offset value as string (e.g., "-2", "0", "1")
            direction: "PRECEDING" or "FOLLOWING"

        Returns:
            SQL ROWS clause fragment
        """
        try:
            offset = int(offset_str)
            if offset == 0:
                return "CURRENT ROW"
            elif offset < 0:
                return f"{abs(offset)} PRECEDING"
            else:
                return f"{offset} FOLLOWING"
        except (ValueError, TypeError):
            logger.warning(f"Invalid window offset: {offset_str}")
            return "CURRENT ROW"

    def _convert_fallback_node(self, node: ASTNode, table_context: str) -> str:
        """
        Convert fallback node to LookML with migration comments.

        This handles cases where the original Tableau formula couldn't be parsed.
        It generates safe LookML with the original formula in comments.

        Args:
            node: Fallback AST node with migration metadata
            table_context: Table context (not used for fallback)

        Returns:
            Safe LookML expression with migration comments
        """
        original_formula = node.properties.get("original_formula", "UNKNOWN")
        parse_error = node.properties.get("parse_error", "Unknown error")

        # Create safe LookML with embedded migration information
        fallback_sql = "'MIGRATION_REQUIRED'"

        # Log detailed migration information (comment will be handled by view generator)
        logger.warning(
            f"Generated fallback LookML for unparseable formula: {original_formula}"
        )
        logger.warning(f"Parse error: {parse_error}")

        return fallback_sql

    def _convert_parameter_ref(self, node: ASTNode, table_context: str) -> str:
        """
        Convert Tableau parameter reference to LookML parameter.

        Args:
            node: Parameter reference AST node
            table_context: Table context (not used for parameters)

        Returns:
            LookML parameter reference in format {% parameter name %}
        """
        if not node.field_name:
            logger.warning("Parameter reference node missing field_name")
            return "/* Invalid parameter reference */"

        # Extract parameter name from field_name (e.g., "parameters.Parameter 10" -> "Parameter 10")
        if "." in node.field_name:
            param_name = node.field_name.split(".", 1)[1]
        else:
            param_name = node.field_name

        # Convert to LookML parameter format: {% parameter name %}
        param_name = self._clean_parameter_name(param_name)

        logger.debug(
            f"Converting parameter reference: {node.field_name} -> {{% parameter {param_name} %}}"
        )
        return f"{{% parameter {param_name} %}}"

    def _clean_parameter_name(self, param_name: str) -> str:
        """
        Clean parameter name to be a valid LookML identifier.

        Args:
            param_name: Raw parameter name from Tableau

        Returns:
            Cleaned parameter name suitable for LookML
        """

        # Replace spaces and special characters (but NOT underscores) with underscores
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "_", param_name)

        # Replace multiple consecutive underscores with single underscore (but preserve existing single underscores)
        clean_name = re.sub(r"_+", "_", clean_name)

        # Remove leading/trailing underscores
        clean_name = clean_name.strip("_")

        # If empty after cleaning, provide a default
        if not clean_name:
            clean_name = "unknown_parameter"

        return clean_name.lower()
