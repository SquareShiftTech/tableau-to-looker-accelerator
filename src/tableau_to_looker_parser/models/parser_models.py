"""
Pydantic models for formula parsing and tokenization.
Separated from parser logic for better organization.
"""

from enum import Enum
from typing import List, Optional, Dict
from pydantic import BaseModel, Field, ConfigDict


class TokenType(Enum):
    """Token types for lexical analysis."""

    # Literals
    STRING = "STRING"
    INTEGER = "INTEGER"
    REAL = "REAL"
    BOOLEAN = "BOOLEAN"
    NULL = "NULL"

    # Identifiers and references
    FIELD_REF = "FIELD_REF"  # [Field Name]
    IDENTIFIER = "IDENTIFIER"  # Function names, keywords

    # Operators
    PLUS = "PLUS"  # +
    MINUS = "MINUS"  # -
    MULTIPLY = "MULTIPLY"  # *
    DIVIDE = "DIVIDE"  # /
    MODULO = "MODULO"  # %
    POWER = "POWER"  # ^

    # Comparison
    EQUAL = "EQUAL"  # =
    NOT_EQUAL = "NOT_EQUAL"  # != or <>
    LESS_THAN = "LESS_THAN"  # <
    LESS_EQUAL = "LESS_EQUAL"  # <=
    GREATER_THAN = "GREATER_THAN"  # >
    GREATER_EQUAL = "GREATER_EQUAL"  # >=

    # Logical
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

    # Punctuation
    LEFT_PAREN = "LEFT_PAREN"  # (
    RIGHT_PAREN = "RIGHT_PAREN"  # )
    COMMA = "COMMA"  # ,

    # Keywords
    IF = "IF"
    THEN = "THEN"
    ELSEIF = "ELSEIF"
    ELSE = "ELSE"
    END = "END"
    CASE = "CASE"
    WHEN = "WHEN"

    # LOD Keywords
    FIXED = "FIXED"
    INCLUDE = "INCLUDE"
    EXCLUDE = "EXCLUDE"

    # LOD Punctuation
    LEFT_BRACE = "LEFT_BRACE"  # {
    RIGHT_BRACE = "RIGHT_BRACE"  # }
    COLON = "COLON"  # :

    # Special
    EOF = "EOF"
    UNKNOWN = "UNKNOWN"


class Token(BaseModel):
    """Token with type, value, and position information."""

    type: TokenType
    value: str
    position: int
    line: int = 1
    column: int = 1


class SupportedOperator(BaseModel):
    """Registry of supported operators."""

    symbol: str
    name: str
    category: str  # "arithmetic", "comparison", "logical"
    precedence: int
    associativity: str = "left"  # "left", "right", "none"
    arity: int = 2  # 1 for unary, 2 for binary

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "symbol": "+",
                    "name": "addition",
                    "category": "arithmetic",
                    "precedence": 4,
                    "associativity": "left",
                    "arity": 2,
                }
            ]
        }
    )


class SupportedFunction(BaseModel):
    """Registry of supported Tableau functions."""

    name: str
    category: str  # "math", "string", "date", "logical", "aggregate", "conversion"
    min_args: int = 0
    max_args: Optional[int] = None  # None = unlimited
    return_type: Optional[str] = None  # DataType as string
    description: str = ""

    # Function properties
    is_aggregate: bool = False
    is_deterministic: bool = True
    requires_context: bool = False  # For LOD expressions

    # Mapping information
    sql_equivalent: Optional[str] = None
    lookml_equivalent: Optional[str] = None

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "SUM",
                    "category": "aggregate",
                    "min_args": 1,
                    "max_args": 1,
                    "return_type": "real",
                    "description": "Sum of all values",
                    "is_aggregate": True,
                    "is_deterministic": True,
                },
                {
                    "name": "UPPER",
                    "category": "string",
                    "min_args": 1,
                    "max_args": 1,
                    "return_type": "string",
                    "description": "Convert text to uppercase",
                    "is_aggregate": False,
                    "is_deterministic": True,
                },
            ]
        }
    )


class FormulaComplexity(BaseModel):
    """Analysis of formula complexity."""

    level: str  # "simple", "medium", "complex"
    score: int  # 1-100
    factors: List[str] = Field(default_factory=list)  # What makes it complex

    # Complexity metrics
    depth: int = 1  # Maximum nesting depth
    node_count: int = 1  # Total AST nodes
    function_count: int = 0  # Number of function calls
    conditional_count: int = 0  # Number of IF/CASE statements

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "level": "simple",
                    "score": 8,
                    "factors": [],
                    "depth": 2,
                    "node_count": 5,
                    "function_count": 0,
                    "conditional_count": 1,
                },
                {
                    "level": "complex",
                    "score": 35,
                    "factors": ["deep nesting", "multiple functions"],
                    "depth": 5,
                    "node_count": 12,
                    "function_count": 3,
                    "conditional_count": 2,
                },
            ]
        }
    )


class ParserError(BaseModel):
    """Structured error information from parser."""

    message: str
    position: int
    line: int = 1
    column: int = 1
    token_value: Optional[str] = None
    expected: Optional[str] = None
    severity: str = "error"  # "error", "warning", "info"

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "message": "Expected ')' after expression",
                    "position": 15,
                    "line": 1,
                    "column": 16,
                    "token_value": "END",
                    "expected": ")",
                    "severity": "error",
                }
            ]
        }
    )


class ParseStatistics(BaseModel):
    """Statistics about the parsing process."""

    tokens_count: int = 0
    ast_nodes_count: int = 0
    parse_time_ms: float = 0.0
    memory_usage_bytes: Optional[int] = None

    # Detailed counts
    literal_count: int = 0
    field_ref_count: int = 0
    function_count: int = 0
    operator_count: int = 0

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "tokens_count": 12,
                    "ast_nodes_count": 7,
                    "parse_time_ms": 2.5,
                    "literal_count": 3,
                    "field_ref_count": 2,
                    "function_count": 1,
                    "operator_count": 1,
                }
            ]
        }
    )


class FunctionRegistry(BaseModel):
    """Central registry of all supported functions."""

    functions: Dict[str, SupportedFunction] = Field(default_factory=dict)
    categories: List[str] = Field(default_factory=list)

    def add_function(self, func: SupportedFunction):
        """Add a function to the registry."""
        self.functions[func.name] = func
        if func.category not in self.categories:
            self.categories.append(func.category)

    def get_function(self, name: str) -> Optional[SupportedFunction]:
        """Get function info by name."""
        return self.functions.get(name.upper())

    def is_supported(self, name: str) -> bool:
        """Check if function is supported."""
        return name.upper() in self.functions

    def get_by_category(self, category: str) -> List[SupportedFunction]:
        """Get all functions in a category."""
        return [func for func in self.functions.values() if func.category == category]

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "functions": {
                        "SUM": {
                            "name": "SUM",
                            "category": "aggregate",
                            "min_args": 1,
                            "max_args": 1,
                            "is_aggregate": True,
                        }
                    },
                    "categories": ["aggregate", "string", "math", "date"],
                }
            ]
        }
    )


class OperatorRegistry(BaseModel):
    """Central registry of all supported operators."""

    operators: Dict[str, SupportedOperator] = Field(default_factory=dict)
    precedence_levels: List[int] = Field(default_factory=list)

    def add_operator(self, op: SupportedOperator):
        """Add an operator to the registry."""
        self.operators[op.symbol] = op
        if op.precedence not in self.precedence_levels:
            self.precedence_levels.append(op.precedence)
            self.precedence_levels.sort()

    def get_operator(self, symbol: str) -> Optional[SupportedOperator]:
        """Get operator info by symbol."""
        return self.operators.get(symbol)

    def get_precedence(self, symbol: str) -> int:
        """Get operator precedence."""
        op = self.operators.get(symbol)
        return op.precedence if op else 0

    def is_supported(self, symbol: str) -> bool:
        """Check if operator is supported."""
        return symbol in self.operators


# Default registries with common functions and operators
def create_default_function_registry() -> FunctionRegistry:
    """Create registry with common Tableau functions."""
    registry = FunctionRegistry()

    # Aggregate functions
    aggregate_functions = [
        SupportedFunction(
            name="SUM",
            category="aggregate",
            min_args=1,
            max_args=1,
            return_type="real",
            is_aggregate=True,
        ),
        SupportedFunction(
            name="COUNT",
            category="aggregate",
            min_args=1,
            max_args=1,
            return_type="integer",
            is_aggregate=True,
        ),
        SupportedFunction(
            name="AVG",
            category="aggregate",
            min_args=1,
            max_args=1,
            return_type="real",
            is_aggregate=True,
        ),
        SupportedFunction(
            name="MIN", category="aggregate", min_args=1, max_args=1, is_aggregate=True
        ),
        SupportedFunction(
            name="MAX", category="aggregate", min_args=1, max_args=1, is_aggregate=True
        ),
    ]

    # String functions
    string_functions = [
        SupportedFunction(
            name="UPPER",
            category="string",
            min_args=1,
            max_args=1,
            return_type="string",
        ),
        SupportedFunction(
            name="LOWER",
            category="string",
            min_args=1,
            max_args=1,
            return_type="string",
        ),
        SupportedFunction(
            name="LEN", category="string", min_args=1, max_args=1, return_type="integer"
        ),
        SupportedFunction(
            name="LEFT", category="string", min_args=2, max_args=2, return_type="string"
        ),
        SupportedFunction(
            name="RIGHT",
            category="string",
            min_args=2,
            max_args=2,
            return_type="string",
        ),
        SupportedFunction(
            name="MID", category="string", min_args=3, max_args=3, return_type="string"
        ),
        # Advanced string functions - Priority 1
        SupportedFunction(
            name="CONTAINS",
            category="string",
            min_args=2,
            max_args=2,
            return_type="boolean",
            description="Check if string contains substring",
        ),
        SupportedFunction(
            name="STARTSWITH",
            category="string",
            min_args=2,
            max_args=2,
            return_type="boolean",
            description="Check if string starts with prefix",
        ),
        SupportedFunction(
            name="ENDSWITH",
            category="string",
            min_args=2,
            max_args=2,
            return_type="boolean",
            description="Check if string ends with suffix",
        ),
        SupportedFunction(
            name="REPLACE",
            category="string",
            min_args=3,
            max_args=3,
            return_type="string",
            description="Replace occurrences of substring",
        ),
        SupportedFunction(
            name="FIND",
            category="string",
            min_args=2,
            max_args=3,
            return_type="integer",
            description="Find position of substring",
        ),
        SupportedFunction(
            name="SPLIT",
            category="string",
            min_args=3,
            max_args=3,
            return_type="string",
            description="Split string and return part by index",
        ),
        SupportedFunction(
            name="LTRIM",
            category="string",
            min_args=1,
            max_args=1,
            return_type="string",
            description="Remove leading whitespace",
        ),
        SupportedFunction(
            name="RTRIM",
            category="string",
            min_args=1,
            max_args=1,
            return_type="string",
            description="Remove trailing whitespace",
        ),
    ]

    # Math functions
    math_functions = [
        SupportedFunction(
            name="ABS", category="math", min_args=1, max_args=1, return_type="real"
        ),
        SupportedFunction(
            name="ROUND", category="math", min_args=1, max_args=2, return_type="real"
        ),
        SupportedFunction(
            name="CEIL", category="math", min_args=1, max_args=1, return_type="real"
        ),
        SupportedFunction(
            name="FLOOR", category="math", min_args=1, max_args=1, return_type="real"
        ),
    ]

    # Date functions
    date_functions = [
        SupportedFunction(
            name="YEAR", category="date", min_args=1, max_args=1, return_type="integer"
        ),
        SupportedFunction(
            name="MONTH", category="date", min_args=1, max_args=1, return_type="integer"
        ),
        SupportedFunction(
            name="DAY", category="date", min_args=1, max_args=1, return_type="integer"
        ),
    ]

    # Logical functions
    logical_functions = [
        SupportedFunction(
            name="IF", category="logical", min_args=3, max_args=3, return_type=None
        ),  # IF(condition, then_value, else_value)
        SupportedFunction(
            name="ISNULL",
            category="logical",
            min_args=1,
            max_args=1,
            return_type="boolean",
        ),
        SupportedFunction(name="IFNULL", category="logical", min_args=2, max_args=2),
    ]

    # Window functions
    window_functions = [
        SupportedFunction(
            name="RUNNING_SUM",
            category="window",
            min_args=1,
            max_args=1,
            return_type="real",
            requires_context=True,
            description="Running sum of values",
        ),
        SupportedFunction(
            name="RUNNING_AVG",
            category="window",
            min_args=1,
            max_args=1,
            return_type="real",
            requires_context=True,
            description="Running average of values",
        ),
        SupportedFunction(
            name="RUNNING_COUNT",
            category="window",
            min_args=1,
            max_args=1,
            return_type="integer",
            requires_context=True,
            description="Running count of values",
        ),
        SupportedFunction(
            name="WINDOW_SUM",
            category="window",
            min_args=3,
            max_args=3,
            return_type="real",
            requires_context=True,
            description="Window sum with range parameters",
        ),
        SupportedFunction(
            name="WINDOW_AVG",
            category="window",
            min_args=3,
            max_args=3,
            return_type="real",
            requires_context=True,
            description="Window average with range parameters",
        ),
        SupportedFunction(
            name="WINDOW_COUNT",
            category="window",
            min_args=3,
            max_args=3,
            return_type="integer",
            requires_context=True,
            description="Window count with range parameters",
        ),
        SupportedFunction(
            name="RANK",
            category="window",
            min_args=1,
            max_args=2,
            return_type="integer",
            requires_context=True,
            description="Rank values with gaps",
        ),
        SupportedFunction(
            name="DENSE_RANK",
            category="window",
            min_args=1,
            max_args=2,
            return_type="integer",
            requires_context=True,
            description="Rank values without gaps",
        ),
        SupportedFunction(
            name="ROW_NUMBER",
            category="window",
            min_args=0,
            max_args=0,
            return_type="integer",
            requires_context=True,
            description="Sequential row number",
        ),
        SupportedFunction(
            name="PERCENTILE",
            category="window",
            min_args=2,
            max_args=2,
            return_type="real",
            requires_context=True,
            description="Percentile calculation",
        ),
        SupportedFunction(
            name="LAG",
            category="window",
            min_args=1,
            max_args=3,
            return_type=None,
            requires_context=True,
            description="Previous row value with offset",
        ),
        SupportedFunction(
            name="LEAD",
            category="window",
            min_args=1,
            max_args=3,
            return_type=None,
            requires_context=True,
            description="Next row value with offset",
        ),
    ]

    # Add all functions
    for func_list in [
        aggregate_functions,
        string_functions,
        math_functions,
        date_functions,
        logical_functions,
        window_functions,
    ]:
        for func in func_list:
            registry.add_function(func)

    return registry


def create_default_operator_registry() -> OperatorRegistry:
    """Create registry with common operators."""
    registry = OperatorRegistry()

    operators = [
        # Arithmetic
        SupportedOperator(
            symbol="+", name="addition", category="arithmetic", precedence=4
        ),
        SupportedOperator(
            symbol="-", name="subtraction", category="arithmetic", precedence=4
        ),
        SupportedOperator(
            symbol="*", name="multiplication", category="arithmetic", precedence=5
        ),
        SupportedOperator(
            symbol="/", name="division", category="arithmetic", precedence=5
        ),
        SupportedOperator(
            symbol="%", name="modulo", category="arithmetic", precedence=5
        ),
        SupportedOperator(
            symbol="^",
            name="power",
            category="arithmetic",
            precedence=6,
            associativity="right",
        ),
        # Comparison
        SupportedOperator(
            symbol="=", name="equal", category="comparison", precedence=3
        ),
        SupportedOperator(
            symbol="!=", name="not_equal", category="comparison", precedence=3
        ),
        SupportedOperator(
            symbol="<>", name="not_equal_alt", category="comparison", precedence=3
        ),
        SupportedOperator(
            symbol="<", name="less_than", category="comparison", precedence=3
        ),
        SupportedOperator(
            symbol="<=", name="less_equal", category="comparison", precedence=3
        ),
        SupportedOperator(
            symbol=">", name="greater_than", category="comparison", precedence=3
        ),
        SupportedOperator(
            symbol=">=", name="greater_equal", category="comparison", precedence=3
        ),
        # Logical
        SupportedOperator(
            symbol="AND", name="logical_and", category="logical", precedence=2
        ),
        SupportedOperator(
            symbol="OR", name="logical_or", category="logical", precedence=1
        ),
        SupportedOperator(
            symbol="NOT", name="logical_not", category="logical", precedence=7, arity=1
        ),
        # Unary
        SupportedOperator(
            symbol="-", name="unary_minus", category="arithmetic", precedence=7, arity=1
        ),
        SupportedOperator(
            symbol="+", name="unary_plus", category="arithmetic", precedence=7, arity=1
        ),
    ]

    for op in operators:
        registry.add_operator(op)

    return registry


# Export all models
__all__ = [
    "TokenType",
    "Token",
    "SupportedOperator",
    "SupportedFunction",
    "FormulaComplexity",
    "ParserError",
    "ParseStatistics",
    "FunctionRegistry",
    "OperatorRegistry",
    "create_default_function_registry",
    "create_default_operator_registry",
]
