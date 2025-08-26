"""
Unified AST Schema for Tableau calculated fields.
Optimized for scalability, maintainability, and future extensions.
"""

from enum import Enum
from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field, ConfigDict


class NodeType(str, Enum):
    """AST node types - extensible for future formula constructs."""

    # Leaf nodes
    FIELD_REF = "field_ref"  # [Field Name]
    PARAMETER_REF = "parameter_ref"  # [Parameters].[Parameter Name]
    LITERAL = "literal"  # "string", 123, true, null

    # Binary operations
    ARITHMETIC = "arithmetic"  # +, -, *, /, %, ^
    COMPARISON = "comparison"  # =, !=, <, >, <=, >=, LIKE, IN
    LOGICAL = "logical"  # AND, OR

    # Control flow
    CONDITIONAL = "conditional"  # IF-THEN-ELSE
    CASE = "case"  # CASE-WHEN-ELSE

    # Function calls
    FUNCTION = "function"  # SUM(), UPPER(), DATEADD(), etc.

    # Complex constructs
    LIST = "list"  # For IN clauses, function args
    RANGE = "range"  # For BETWEEN clauses
    LOD_EXPRESSION = "lod_expression"  # {FIXED/INCLUDE/EXCLUDE [dims] : AGG([field])}
    WINDOW_FUNCTION = "window_function"  # RUNNING_SUM(), RANK(), WINDOW_SUM(), etc.
    DERIVED_TABLE = "derived_table"

    # Unary operations
    UNARY = "unary"  # NOT, -, +


class DataType(str, Enum):
    """Data types for type inference and validation."""

    STRING = "string"
    INTEGER = "integer"
    REAL = "real"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    NULL = "null"
    UNKNOWN = "unknown"  # For complex expressions


class ASTNode(BaseModel):
    """
    Unified AST node that can represent any Tableau formula construct.
    Uses composition pattern for maximum flexibility and future extensibility.
    """

    # Core identification
    node_type: NodeType
    data_type: Optional[DataType] = None

    # Binary operation fields
    operator: Optional[str] = None
    left: Optional["ASTNode"] = None
    right: Optional["ASTNode"] = None

    # Unary operation fields
    operand: Optional["ASTNode"] = None

    # Control flow fields
    condition: Optional["ASTNode"] = None
    then_branch: Optional["ASTNode"] = None
    else_branch: Optional["ASTNode"] = None

    # Case statement fields
    case_expression: Optional["ASTNode"] = None  # CASE [field] WHEN ...
    when_clauses: List["WhenClause"] = Field(default_factory=list)

    # Function call fields
    function_name: Optional[str] = None
    arguments: List["ASTNode"] = Field(default_factory=list)

    # Field reference fields
    field_name: Optional[str] = None
    table_name: Optional[str] = None
    original_name: Optional[str] = None  # Original Tableau name with brackets

    # Literal value fields
    value: Optional[Union[str, int, float, bool]] = None

    # List/collection fields
    items: List["ASTNode"] = Field(default_factory=list)

    # Range fields (for BETWEEN)
    min_value: Optional["ASTNode"] = None
    max_value: Optional["ASTNode"] = None

    # LOD expression fields
    lod_type: Optional[str] = None  # "FIXED", "INCLUDE", "EXCLUDE"
    lod_dimensions: List["ASTNode"] = Field(
        default_factory=list
    )  # [Region], [Category]
    lod_expression: Optional["ASTNode"] = None  # SUM([Sales]), AVG([Profit])

    # Window function fields
    window_function_type: Optional[str] = None  # "RUNNING_SUM", "RANK", "WINDOW_SUM"
    partition_by: List["ASTNode"] = Field(default_factory=list)  # PARTITION BY fields
    order_by: List["ASTNode"] = Field(default_factory=list)  # ORDER BY fields
    window_frame: Optional[str] = None  # "ROWS UNBOUNDED PRECEDING", etc.

    # Metadata and extensions
    confidence: float = 1.0
    source_location: Optional[str] = None  # Original position in formula
    properties: Dict[str, Any] = Field(default_factory=dict)  # Future extensions

    model_config = ConfigDict(
        use_enum_values=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "node_type": "field_ref",
                    "field_name": "sales",
                    "original_name": "[Sales]",
                    "data_type": "real",
                },
                {"node_type": "literal", "value": 1000, "data_type": "integer"},
                {
                    "node_type": "arithmetic",
                    "operator": "+",
                    "left": {"node_type": "field_ref", "field_name": "sales"},
                    "right": {"node_type": "field_ref", "field_name": "profit"},
                    "data_type": "real",
                },
                {
                    "node_type": "conditional",
                    "condition": {
                        "node_type": "comparison",
                        "operator": ">",
                        "left": {"node_type": "field_ref", "field_name": "sales"},
                        "right": {"node_type": "literal", "value": 1000},
                    },
                    "then_branch": {"node_type": "literal", "value": "High"},
                    "else_branch": {"node_type": "literal", "value": "Low"},
                    "data_type": "string",
                },
                {
                    "node_type": "function",
                    "function_name": "UPPER",
                    "arguments": [
                        {"node_type": "field_ref", "field_name": "customer_name"}
                    ],
                    "data_type": "string",
                },
            ]
        },
    )


class WhenClause(BaseModel):
    """WHEN clause for CASE statements."""

    condition: ASTNode
    result: ASTNode


class CalculatedField(BaseModel):
    """Complete representation of a calculated field with AST."""

    # Basic field information
    name: str
    original_formula: str
    field_type: str  # "dimension" or "measure"

    # AST representation
    ast_root: ASTNode

    # Metadata
    data_type: DataType = DataType.UNKNOWN
    complexity: str = "simple"  # "simple", "medium", "complex"
    dependencies: List[str] = Field(default_factory=list)  # Referenced field names

    # Analysis results
    requires_aggregation: bool = False
    is_deterministic: bool = True
    has_table_calc: bool = False

    # Quality metrics
    parse_confidence: float = 1.0
    validation_errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    # Additional metadata and extensions
    properties: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "sales_category",
                    "original_formula": 'IF [Sales] > 1000 THEN "High" ELSE "Low" END',
                    "field_type": "dimension",
                    "ast_root": {
                        "node_type": "conditional",
                        "condition": {
                            "node_type": "comparison",
                            "operator": ">",
                            "left": {"node_type": "field_ref", "field_name": "sales"},
                            "right": {"node_type": "literal", "value": 1000},
                        },
                        "then_branch": {"node_type": "literal", "value": "High"},
                        "else_branch": {"node_type": "literal", "value": "Low"},
                    },
                    "data_type": "string",
                    "complexity": "simple",
                    "dependencies": ["sales"],
                    "parse_confidence": 0.95,
                }
            ]
        }
    )


class FormulaParseResult(BaseModel):
    """Result of parsing a Tableau formula."""

    success: bool
    original_formula: str

    # Success case
    calculated_field: Optional[CalculatedField] = None

    # Error case
    error_message: Optional[str] = None
    error_location: Optional[str] = None
    suggestions: List[str] = Field(default_factory=list)

    # Metadata
    parse_time_ms: float = 0.0
    tokens_count: int = 0
    ast_nodes_count: int = 0


class SupportedOperator(BaseModel):
    """Registry of supported operators."""

    symbol: str
    name: str
    category: str  # "arithmetic", "comparison", "logical"
    precedence: int
    associativity: str = "left"  # "left", "right", "none"
    arity: int = 2  # 1 for unary, 2 for binary


class SupportedFunction(BaseModel):
    """Registry of supported Tableau functions."""

    name: str
    category: str  # "math", "string", "date", "logical", "aggregate", "conversion"
    min_args: int = 0
    max_args: Optional[int] = None  # None = unlimited
    return_type: Optional[DataType] = None
    description: str = ""

    # Function properties
    is_aggregate: bool = False
    is_deterministic: bool = True
    requires_context: bool = False  # For LOD expressions

    # Mapping information
    sql_equivalent: Optional[str] = None
    lookml_equivalent: Optional[str] = None


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


class ASTValidator:
    """Validator for AST structure integrity."""

    @staticmethod
    def validate_node(node: ASTNode) -> List[str]:
        """Validate AST node structure and return any errors."""
        errors = []

        # Type-specific validation
        if node.node_type == NodeType.ARITHMETIC:
            if not (node.operator and node.left and node.right):
                errors.append("Arithmetic node missing operator or operands")

        elif node.node_type == NodeType.FIELD_REF:
            if not node.field_name:
                errors.append("Field reference missing field_name")

        elif node.node_type == NodeType.LITERAL:
            if node.value is None and node.data_type != DataType.NULL:
                errors.append("Literal node missing value")

        elif node.node_type == NodeType.FUNCTION:
            if not node.function_name:
                errors.append("Function node missing function_name")

        elif node.node_type == NodeType.DERIVED_TABLE:
            if not node.function_name:
                errors.append("Function node missing function_name")

        elif node.node_type == NodeType.CONDITIONAL:
            if not (node.condition and node.then_branch):
                errors.append("Conditional node missing condition or then_branch")

        return errors

    @staticmethod
    def validate_ast(root: ASTNode) -> List[str]:
        """Recursively validate entire AST."""
        errors = []

        def visit(node: ASTNode):
            errors.extend(ASTValidator.validate_node(node))

            # Recursively validate child nodes
            for child in [
                node.left,
                node.right,
                node.operand,
                node.condition,
                node.then_branch,
                node.else_branch,
                node.case_expression,
                node.min_value,
                node.max_value,
            ]:
                if child:
                    visit(child)

            for child_list in [node.arguments, node.items]:
                for child in child_list:
                    visit(child)

            for when_clause in node.when_clauses:
                visit(when_clause.condition)
                visit(when_clause.result)

        visit(root)
        return errors


# Forward reference resolution
ASTNode.model_rebuild()
WhenClause.model_rebuild()


# Export main classes
__all__ = [
    "NodeType",
    "DataType",
    "ASTNode",
    "WhenClause",
    "CalculatedField",
    "FormulaParseResult",
    "SupportedOperator",
    "SupportedFunction",
    "FormulaComplexity",
    "ASTValidator",
]
