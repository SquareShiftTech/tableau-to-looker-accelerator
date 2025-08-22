"""
Tableau Formula Parser - Convert Tableau formulas to AST.
Handles tokenization, parsing, and AST generation with comprehensive error handling.
"""

import re
import logging
from typing import List, Optional

from ..models.ast_schema import (
    ASTNode,
    NodeType,
    DataType,
    CalculatedField,
    FormulaParseResult,
    ASTValidator,
)
from ..models.parser_models import (
    TokenType,
    Token,
    FunctionRegistry,
    OperatorRegistry,
    ParseStatistics,
    ParserError,
    FormulaComplexity,
    create_default_function_registry,
    create_default_operator_registry,
)

logger = logging.getLogger(__name__)


class FormulaLexer:
    """Tokenizer for Tableau formulas."""

    # Token patterns (order matters!)
    TOKEN_PATTERNS = [
        # String literals
        (r'"([^"\\]|\\.)*"', TokenType.STRING),
        (r"'([^'\\]|\\.)*'", TokenType.STRING),
        # Field references
        (r"\[([^\]]+)\]", TokenType.FIELD_REF),
        # Numbers
        (r"\d+\.\d+", TokenType.REAL),
        (r"\d+", TokenType.INTEGER),
        # Multi-character operators
        (r"!=|<>", TokenType.NOT_EQUAL),
        (r"<=", TokenType.LESS_EQUAL),
        (r">=", TokenType.GREATER_EQUAL),
        # Single character operators
        (r"\+", TokenType.PLUS),
        (r"-", TokenType.MINUS),
        (r"\*", TokenType.MULTIPLY),
        (r"/", TokenType.DIVIDE),
        (r"%", TokenType.MODULO),
        (r"\^", TokenType.POWER),
        (r"=", TokenType.EQUAL),
        (r"<", TokenType.LESS_THAN),
        (r">", TokenType.GREATER_THAN),
        # Punctuation
        (r"\(", TokenType.LEFT_PAREN),
        (r"\)", TokenType.RIGHT_PAREN),
        (r",", TokenType.COMMA),
        (r"\{", TokenType.LEFT_BRACE),
        (r"\}", TokenType.RIGHT_BRACE),
        (r":", TokenType.COLON),
        # Keywords and identifiers (case insensitive)
        (r"(?i)\bIF\b", TokenType.IF),
        (r"(?i)\bTHEN\b", TokenType.THEN),
        (r"(?i)\bELSEIF\b", TokenType.ELSEIF),
        (r"(?i)\bELSE\b", TokenType.ELSE),
        (r"(?i)\bEND\b", TokenType.END),
        (r"(?i)\bCASE\b", TokenType.CASE),
        (r"(?i)\bWHEN\b", TokenType.WHEN),
        (r"(?i)\bFIXED\b", TokenType.FIXED),
        (r"(?i)\bINCLUDE\b", TokenType.INCLUDE),
        (r"(?i)\bEXCLUDE\b", TokenType.EXCLUDE),
        (r"(?i)\bAND\b", TokenType.AND),
        (r"(?i)\bOR\b", TokenType.OR),
        (r"(?i)\bNOT\b", TokenType.NOT),
        (r"(?i)\bTRUE\b", TokenType.BOOLEAN),
        (r"(?i)\bFALSE\b", TokenType.BOOLEAN),
        (r"(?i)\bNULL\b", TokenType.NULL),
        # Identifiers (function names, etc.)
        (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER),
    ]

    def __init__(self):
        # Compile patterns for performance
        self.compiled_patterns = [
            (re.compile(pattern), token_type)
            for pattern, token_type in self.TOKEN_PATTERNS
        ]

    def tokenize(self, formula: str) -> List[Token]:
        """Tokenize a Tableau formula string."""
        tokens = []
        position = 0
        line = 1
        column = 1

        while position < len(formula):
            # Skip whitespace
            if formula[position].isspace():
                if formula[position] == "\n":
                    line += 1
                    column = 1
                else:
                    column += 1
                position += 1
                continue

            # Try to match a token
            matched = False
            for pattern, token_type in self.compiled_patterns:
                match = pattern.match(formula, position)
                if match:
                    value = match.group(0)

                    # Special handling for certain token types
                    if token_type == TokenType.FIELD_REF:
                        # Extract field name without brackets
                        value = match.group(1)
                    elif token_type == TokenType.STRING:
                        # Remove quotes from string literals
                        value = value[1:-1]
                    elif token_type == TokenType.BOOLEAN:
                        # Normalize boolean values
                        value = value.upper()

                    tokens.append(
                        Token(
                            type=token_type,
                            value=value,
                            position=position,
                            line=line,
                            column=column,
                        )
                    )

                    position = match.end()
                    column += len(match.group(0))
                    matched = True
                    break

            if not matched:
                # Unknown character
                tokens.append(
                    Token(
                        type=TokenType.UNKNOWN,
                        value=formula[position],
                        position=position,
                        line=line,
                        column=column,
                    )
                )
                position += 1
                column += 1

        # Add EOF token
        tokens.append(
            Token(
                type=TokenType.EOF,
                value="",
                position=position,
                line=line,
                column=column,
            )
        )

        return tokens


class FormulaParser:
    """Recursive descent parser for Tableau formulas."""

    # Operator precedence (higher number = higher precedence)
    PRECEDENCE = {
        TokenType.OR: 1,
        TokenType.AND: 2,
        TokenType.EQUAL: 3,
        TokenType.NOT_EQUAL: 3,
        TokenType.LESS_THAN: 3,
        TokenType.LESS_EQUAL: 3,
        TokenType.GREATER_THAN: 3,
        TokenType.GREATER_EQUAL: 3,
        TokenType.PLUS: 4,
        TokenType.MINUS: 4,
        TokenType.MULTIPLY: 5,
        TokenType.DIVIDE: 5,
        TokenType.MODULO: 5,
        TokenType.POWER: 6,
    }

    def __init__(
        self,
        function_registry: Optional[FunctionRegistry] = None,
        operator_registry: Optional[OperatorRegistry] = None,
    ):
        self.lexer = FormulaLexer()
        self.tokens: List[Token] = []
        self.current = 0
        self.errors: List[ParserError] = []
        self.warnings: List[ParserError] = []

        # Use provided registries or create defaults
        self.function_registry = function_registry or create_default_function_registry()
        self.operator_registry = operator_registry or create_default_operator_registry()

        # Statistics
        self.stats = ParseStatistics()

    def parse_formula(
        self, formula: str, field_name: str = "", field_type: str = "dimension"
    ) -> FormulaParseResult:
        """Parse a Tableau formula and return the result."""
        try:
            # comment = ""
            formula = formula
            if "//" in formula:
                parts = formula.split("//", 1)
                formula = parts[0].rstrip()
                # comment = "//" + parts[1].strip()
            # Reset state
            self.tokens = self.lexer.tokenize(formula)
            self.current = 0
            self.errors = []
            self.warnings = []

            logger.info(f"Parsing formula: {formula}")

            # Parse the expression
            ast_root = self.parse_expression()

            if self.errors:
                error_messages = [err.message for err in self.errors]
                error_message = "; ".join(error_messages)

                # Create fallback calculated field for parsing errors too
                fallback_field = self._create_fallback_calculated_field(
                    formula, field_name, field_type, error_message
                )

                return FormulaParseResult(
                    success=False,
                    original_formula=formula,
                    error_message=error_message,
                    calculated_field=fallback_field,  # Provide fallback for parsing errors
                )

            # Analyze the AST
            dependencies = self._extract_dependencies(ast_root)
            complexity = self._analyze_complexity(ast_root)
            data_type = self._infer_data_type(ast_root)

            # Create calculated field
            calculated_field = CalculatedField(
                name=field_name or "unnamed_field",
                original_formula=formula,
                field_type=field_type,
                ast_root=ast_root,
                data_type=data_type,
                complexity=complexity.level,
                dependencies=dependencies,
                requires_aggregation=self._has_aggregation(ast_root),
                is_deterministic=self._is_deterministic(ast_root),
                parse_confidence=self._calculate_confidence(),
                validation_errors=[err.message for err in self.errors],
                warnings=[warn.message for warn in self.warnings],
            )

            # Validate AST structure
            validation_errors = ASTValidator.validate_ast(ast_root)
            if validation_errors:
                calculated_field.validation_errors.extend(validation_errors)

            return FormulaParseResult(
                success=True,
                original_formula=formula,
                calculated_field=calculated_field,
                tokens_count=len(self.tokens) - 1,  # Exclude EOF
                ast_nodes_count=self._count_nodes(ast_root),
            )

        except Exception as e:
            logger.error(f"Critical parse error for formula '{formula}': {str(e)}")

            # Create fallback calculated field for graceful degradation
            fallback_field = self._create_fallback_calculated_field(
                formula, field_name, field_type, str(e)
            )

            return FormulaParseResult(
                success=False,
                original_formula=formula,
                error_message=str(e),
                calculated_field=fallback_field,  # Provide fallback even on failure
            )

    def parse_expression(self) -> ASTNode:
        """Parse a complete expression."""
        return self.parse_or_expression()

    def parse_or_expression(self) -> ASTNode:
        """Parse OR expressions."""
        left = self.parse_and_expression()

        while self.match(TokenType.OR):
            operator = self.previous().value
            right = self.parse_and_expression()
            left = ASTNode(
                node_type=NodeType.LOGICAL, operator=operator, left=left, right=right
            )

        return left

    def parse_and_expression(self) -> ASTNode:
        """Parse AND expressions."""
        left = self.parse_equality()

        while self.match(TokenType.AND):
            operator = self.previous().value
            right = self.parse_equality()
            left = ASTNode(
                node_type=NodeType.LOGICAL, operator=operator, left=left, right=right
            )

        return left

    def parse_equality(self) -> ASTNode:
        """Parse equality and comparison expressions."""
        left = self.parse_comparison()

        while self.match(TokenType.EQUAL, TokenType.NOT_EQUAL):
            operator = self.previous().value
            right = self.parse_comparison()
            left = ASTNode(
                node_type=NodeType.COMPARISON, operator=operator, left=left, right=right
            )

        return left

    def parse_comparison(self) -> ASTNode:
        """Parse comparison expressions."""
        left = self.parse_term()

        while self.match(
            TokenType.GREATER_THAN,
            TokenType.GREATER_EQUAL,
            TokenType.LESS_THAN,
            TokenType.LESS_EQUAL,
        ):
            operator = self.previous().value
            right = self.parse_term()
            left = ASTNode(
                node_type=NodeType.COMPARISON, operator=operator, left=left, right=right
            )

        return left

    def parse_term(self) -> ASTNode:
        """Parse addition and subtraction."""
        left = self.parse_factor()

        while self.match(TokenType.PLUS, TokenType.MINUS):
            operator = self.previous().value
            right = self.parse_factor()
            left = ASTNode(
                node_type=NodeType.ARITHMETIC, operator=operator, left=left, right=right
            )

        return left

    def parse_factor(self) -> ASTNode:
        """Parse multiplication, division, and modulo."""
        left = self.parse_unary()

        while self.match(TokenType.MULTIPLY, TokenType.DIVIDE, TokenType.MODULO):
            operator = self.previous().value
            right = self.parse_unary()
            left = ASTNode(
                node_type=NodeType.ARITHMETIC, operator=operator, left=left, right=right
            )

        return left

    def parse_unary(self) -> ASTNode:
        """Parse unary expressions."""
        if self.match(TokenType.NOT, TokenType.MINUS):
            operator = self.previous().value
            operand = self.parse_unary()
            return ASTNode(node_type=NodeType.UNARY, operator=operator, operand=operand)

        return self.parse_power()

    def parse_power(self) -> ASTNode:
        """Parse power expressions."""
        left = self.parse_primary()

        if self.match(TokenType.POWER):
            operator = self.previous().value
            right = self.parse_unary()  # Right associative
            left = ASTNode(
                node_type=NodeType.ARITHMETIC, operator=operator, left=left, right=right
            )

        return left

    def parse_primary(self) -> ASTNode:
        """Parse primary expressions."""
        # IF statement
        if self.match(TokenType.IF):
            return self.parse_if_statement()

        # CASE statement
        if self.match(TokenType.CASE):
            return self.parse_case_statement()

        # LOD expression
        if self.match(TokenType.LEFT_BRACE):
            # Peek ahead to check if FIXED, INCLUDE, or EXCLUDE follows
            if (
                self.check(TokenType.FIXED)
                or self.check(TokenType.INCLUDE)
                or self.check(TokenType.EXCLUDE)
            ):
                return self.parse_lod_expression()
            else:
                return self.parse_derived_table_expression()

        # Parenthesized expression
        if self.match(TokenType.LEFT_PAREN):
            expr = self.parse_expression()
            self.consume(TokenType.RIGHT_PAREN, "Expected ')' after expression")
            return expr

        # Literals
        if self.match(TokenType.STRING):
            return ASTNode(
                node_type=NodeType.LITERAL,
                value=self.previous().value,
                data_type=DataType.STRING,
            )

        if self.match(TokenType.INTEGER):
            return ASTNode(
                node_type=NodeType.LITERAL,
                value=int(self.previous().value),
                data_type=DataType.INTEGER,
            )

        if self.match(TokenType.REAL):
            return ASTNode(
                node_type=NodeType.LITERAL,
                value=float(self.previous().value),
                data_type=DataType.REAL,
            )

        if self.match(TokenType.BOOLEAN):
            value = self.previous().value.upper() == "TRUE"
            return ASTNode(
                node_type=NodeType.LITERAL, value=value, data_type=DataType.BOOLEAN
            )

        if self.match(TokenType.NULL):
            return ASTNode(
                node_type=NodeType.LITERAL, value=None, data_type=DataType.NULL
            )

        # Field reference
        if self.match(TokenType.FIELD_REF):
            field_name = self.previous().value

            # To DO : remove harcoded fix
            if field_name == "Rolling 36 (copy)_777433916922368001":
                processed_field_name = "max dttm"
            else:
                processed_field_name = field_name.lower().replace(" ", "_")

            return ASTNode(
                node_type=NodeType.FIELD_REF,
                field_name=processed_field_name,
                original_name=f"[{field_name}]",
            )

        # Function call
        if self.match(TokenType.IDENTIFIER):
            return self.parse_function_call()

        # Error case
        current_token = self.peek()
        self.errors.append(
            ParserError(
                message=f"Unexpected token: {current_token.value}",
                position=current_token.position,
                token_value=current_token.value,
                severity="error",
            )
        )
        return ASTNode(node_type=NodeType.LITERAL, value=None, data_type=DataType.NULL)

    def parse_if_statement(self) -> ASTNode:
        """Parse IF-THEN-ELSEIF-ELSE statement with multiple ELSEIF support."""
        condition = self.parse_expression()
        self.consume(TokenType.THEN, "Expected 'THEN' after IF condition")
        then_branch = self.parse_expression()

        else_branch = None

        # Handle multiple ELSEIF clauses by creating nested conditionals
        while self.match(TokenType.ELSEIF):
            elseif_condition = self.parse_expression()
            self.consume(TokenType.THEN, "Expected 'THEN' after ELSEIF condition")
            elseif_then = self.parse_expression()

            # Create nested conditional for ELSEIF
            nested_conditional = ASTNode(
                node_type=NodeType.CONDITIONAL,
                condition=elseif_condition,
                then_branch=elseif_then,
                else_branch=None,  # Will be set by next ELSEIF or ELSE
            )

            if else_branch is None:
                else_branch = nested_conditional
            else:
                # Find the deepest else_branch and attach there
                current = else_branch
                while current.else_branch is not None:
                    current = current.else_branch
                current.else_branch = nested_conditional

        # Handle final ELSE clause
        if self.match(TokenType.ELSE):
            final_else = self.parse_expression()
            if else_branch is None:
                else_branch = final_else
            else:
                # Find the deepest else_branch and attach there
                current = else_branch
                while current.else_branch is not None:
                    current = current.else_branch
                current.else_branch = final_else

        self.consume(TokenType.END, "Expected 'END' to close IF statement")

        return ASTNode(
            node_type=NodeType.CONDITIONAL,
            condition=condition,
            then_branch=then_branch,
            else_branch=else_branch,
        )

    def parse_case_statement(self) -> ASTNode:
        """Parse CASE statement with full WHEN clause support."""
        from ..models.ast_schema import WhenClause

        case_expression = None
        when_clauses = []
        else_branch = None

        # Check if this is CASE [expression] WHEN... or CASE WHEN...
        if not self.check(TokenType.WHEN):
            # CASE [expression] WHEN... format
            case_expression = self.parse_expression()

        # Parse WHEN clauses
        while self.match(TokenType.WHEN):
            when_condition = self.parse_expression()
            self.consume(TokenType.THEN, "Expected 'THEN' after WHEN condition")
            when_result = self.parse_expression()

            when_clauses.append(
                WhenClause(condition=when_condition, result=when_result)
            )

        # Parse optional ELSE clause
        if self.match(TokenType.ELSE):
            else_branch = self.parse_expression()

        self.consume(TokenType.END, "Expected 'END' to close CASE statement")

        return ASTNode(
            node_type=NodeType.CASE,
            case_expression=case_expression,
            when_clauses=when_clauses,
            else_branch=else_branch,
        )

    def parse_lod_expression(self) -> ASTNode:
        """Parse LOD expression: {FIXED/INCLUDE/EXCLUDE [dims] : AGG([field])}"""
        # Consume LOD type (FIXED, INCLUDE, EXCLUDE)
        lod_type = None
        if self.match(TokenType.FIXED, TokenType.INCLUDE, TokenType.EXCLUDE):
            lod_type = self.previous().value.upper()
        else:
            self.errors.append(
                ParserError(
                    message="Expected FIXED, INCLUDE, or EXCLUDE after '{'",
                    position=self.peek().position,
                    severity="error",
                )
            )
            return ASTNode(
                node_type=NodeType.LITERAL, value=None, data_type=DataType.NULL
            )

        # Parse dimensions: [Region], [Category] (optional for some LOD types)
        dimensions = []
        while self.check(TokenType.FIELD_REF):
            dim = self.parse_field_reference()
            dimensions.append(dim)
            if not self.match(TokenType.COMMA):
                break

        # Consume colon (:)
        self.consume(TokenType.COLON, "Expected ':' after LOD dimensions")

        # Parse the aggregation expression
        lod_expression = self.parse_expression()

        # Consume closing brace
        self.consume(TokenType.RIGHT_BRACE, "Expected '}' to close LOD expression")

        return ASTNode(
            node_type=NodeType.LOD_EXPRESSION,
            lod_type=lod_type,
            lod_dimensions=dimensions,
            lod_expression=lod_expression,
        )

    def parse_derived_table_expression(self) -> ASTNode:
        """
        Parses non-LOD expressions wrapped in `{}` like {MAX([DTTM])}.
        These are treated as derived table expressions.
        """
        # Parse the inner expression (e.g., MAX([DTTM]))
        wrapped_expr = self.parse_expression()

        # Consume the closing brace
        self.consume(TokenType.RIGHT_BRACE, "Expected '}' to close derived expression")

        # Validate structure: must be a function call on a single FIELD_REF
        if (
            wrapped_expr.node_type == NodeType.FUNCTION
            and len(wrapped_expr.arguments) == 1
            and wrapped_expr.arguments[0].node_type == NodeType.FIELD_REF
        ):
            func_name = wrapped_expr.function_name.upper()
            field_node = wrapped_expr.arguments[0]

            return ASTNode(
                node_type=NodeType.DERIVED_TABLE,
                properties={
                    "aggregation_function": func_name,
                    "field_name": field_node.field_name,
                    "original_field": field_node.original_name,
                    "table_alias": "base",
                    "derived_table_alias": f"{func_name.lower()}_table",
                    "derived_field_alias": f"{func_name.capitalize()}Date",  # e.g., MaxDate
                },
            )

        # If the structure doesn't match the pattern, fallback to literal/null
        self.errors.append(
            ParserError(
                message="Expected aggregation function on a single field inside '{...}'",
                position=self.peek().position,
                severity="error",
            )
        )
        return ASTNode(
            node_type=NodeType.LITERAL,
            value=None,
            data_type=DataType.NULL,
        )

    def parse_field_reference(self) -> ASTNode:
        """Parse a field reference like [Field Name]."""
        field_name = self.advance().value  # Get the field name
        return ASTNode(
            node_type=NodeType.FIELD_REF,
            field_name=field_name.lower().replace(" ", "_"),
            original_name=f"[{field_name}]",
        )

    def parse_function_call(self) -> ASTNode:
        """Parse function call."""
        func_name = self.previous().value.upper()

        # Check if function is supported
        func_info = self.function_registry.get_function(func_name)
        if not func_info:
            self.warnings.append(
                ParserError(
                    message=f"Unsupported function: {func_name}",
                    position=self.previous().position,
                    severity="warning",
                )
            )

        # Check if this is a window function
        is_window_function = func_info and func_info.category == "window"

        self.consume(
            TokenType.LEFT_PAREN, f"Expected '(' after function name {func_name}"
        )

        arguments = []
        if not self.check(TokenType.RIGHT_PAREN):
            arguments.append(self.parse_expression())
            while self.match(TokenType.COMMA):
                arguments.append(self.parse_expression())

        self.consume(TokenType.RIGHT_PAREN, "Expected ')' after function arguments")

        # For window functions, create a WINDOW_FUNCTION node type
        if is_window_function:
            return ASTNode(
                node_type=NodeType.WINDOW_FUNCTION,
                window_function_type=func_name,
                arguments=arguments,
                # Default window properties - can be extended later for OVER clauses
                partition_by=[],
                order_by=[],
                window_frame=None,
            )
        else:
            return ASTNode(
                node_type=NodeType.FUNCTION,
                function_name=func_name,
                arguments=arguments,
            )

    # Helper methods
    def match(self, *types: TokenType) -> bool:
        """Check if current token matches any of the given types."""
        for token_type in types:
            if self.check(token_type):
                self.advance()
                return True
        return False

    def check(self, token_type: TokenType) -> bool:
        """Check if current token is of given type."""
        if self.is_at_end():
            return False
        return self.peek().type == token_type

    def advance(self) -> Token:
        """Consume and return current token."""
        if not self.is_at_end():
            self.current += 1
        return self.previous()

    def is_at_end(self) -> bool:
        """Check if we've reached the end."""
        return self.peek().type == TokenType.EOF

    def peek(self) -> Token:
        """Return current token without consuming it."""
        return self.tokens[self.current]

    def previous(self) -> Token:
        """Return previous token."""
        return self.tokens[self.current - 1]

    def consume(self, token_type: TokenType, message: str) -> Token:
        """Consume token of expected type or add error."""
        if self.check(token_type):
            return self.advance()

        current_token = self.peek()
        self.errors.append(
            ParserError(
                message=f"{message}. Got {current_token.value}",
                position=current_token.position,
                token_value=current_token.value,
                severity="error",
            )
        )
        return current_token

    # Analysis methods
    def _extract_dependencies(self, node: ASTNode) -> List[str]:
        """Extract field dependencies from AST."""
        dependencies = set()

        def visit(n: ASTNode):
            if n.node_type == NodeType.FIELD_REF and n.field_name:
                dependencies.add(n.field_name)
            if n.node_type == NodeType.DERIVED_TABLE:
                n.field_name = n.properties["field_name"]
                dependencies.add(n.field_name)

            # Visit child nodes
            for child in [
                n.left,
                n.right,
                n.operand,
                n.condition,
                n.then_branch,
                n.else_branch,
            ]:
                if child:
                    visit(child)

            for child in n.arguments:
                visit(child)

        visit(node)
        return sorted(list(dependencies))

    def _analyze_complexity(self, node: ASTNode) -> FormulaComplexity:
        """Analyze formula complexity."""
        node_count = self._count_nodes(node)
        depth = self._calculate_depth(node)
        function_count = self._count_functions(node)
        conditional_count = self._count_conditionals(node)

        # Calculate complexity score
        score = node_count + depth * 2 + function_count * 3 + conditional_count * 5

        if score <= 15:
            level = "simple"
        elif score <= 30:
            level = "medium"
        else:
            level = "complex"

        factors = []
        if depth > 3:
            factors.append("deep nesting")
        if function_count > 2:
            factors.append("multiple functions")
        if conditional_count > 1:
            factors.append("complex logic")

        return FormulaComplexity(
            level=level,
            score=score,
            factors=factors,
            depth=depth,
            node_count=node_count,
            function_count=function_count,
            conditional_count=conditional_count,
        )

    def _infer_data_type(self, node: ASTNode) -> DataType:
        """Infer the data type of the expression."""
        if node.data_type:
            return node.data_type

        if node.node_type == NodeType.ARITHMETIC:
            # Arithmetic operations typically return numbers
            return DataType.REAL
        elif node.node_type == NodeType.COMPARISON:
            # Comparisons return boolean
            return DataType.BOOLEAN
        elif node.node_type == NodeType.CONDITIONAL:
            # IF statements return type of branches
            if node.then_branch and node.then_branch.data_type:
                return node.then_branch.data_type
        elif node.node_type == NodeType.FUNCTION and node.function_name:
            # Look up function return type
            func_info = self.function_registry.get_function(node.function_name)
            if func_info and func_info.return_type:
                # Convert string return type back to DataType enum
                return (
                    DataType(func_info.return_type)
                    if func_info.return_type in DataType._value2member_map_
                    else DataType.UNKNOWN
                )

        return DataType.UNKNOWN

    def _has_aggregation(self, node: ASTNode) -> bool:
        """Check if expression contains aggregation functions."""
        if node.node_type == NodeType.FUNCTION and node.function_name:
            func_info = self.function_registry.get_function(node.function_name)
            if func_info and func_info.is_aggregate:
                return True

        # Check child nodes
        for child in [
            node.left,
            node.right,
            node.operand,
            node.condition,
            node.then_branch,
            node.else_branch,
        ]:
            if child and self._has_aggregation(child):
                return True

        for child in node.arguments:
            if self._has_aggregation(child):
                return True

        return False

    def _create_fallback_calculated_field(
        self, formula: str, field_name: str, field_type: str, error_message: str
    ) -> CalculatedField:
        """
        Create a fallback calculated field when parsing fails.

        This ensures LookML generation can continue even with unparseable formulas.
        The fallback includes the original formula as metadata for manual migration.
        """
        # Create a simple fallback AST node that represents the unparseable formula
        fallback_ast = ASTNode(
            node_type=NodeType.LITERAL,
            value="MIGRATION_REQUIRED",
            data_type=DataType.STRING,
            properties={
                "original_formula": formula,
                "parse_error": error_message,
                "migration_status": "MANUAL_REQUIRED",
                "migration_comment": f"Original Tableau formula: {formula}",
            },
        )

        # Create calculated field with error metadata
        calculated_field = CalculatedField(
            name=field_name or "unparseable_field",
            original_formula=formula,
            field_type=field_type,
            ast_root=fallback_ast,
            data_type=DataType.STRING,  # Safe default
            complexity="error",
            dependencies=[],  # Can't extract dependencies from failed parse
            requires_aggregation=False,  # Safe default
            is_deterministic=True,  # Safe default
            parse_confidence=0.0,  # Zero confidence for failed parse
            validation_errors=[f"Parse failure: {error_message}"],
            warnings=[f"Fallback generated for formula: {formula}"],
        )

        logger.warning(f"Created fallback calculated field for: {formula}")
        return calculated_field

    def _is_deterministic(self, node: ASTNode) -> bool:
        """Check if expression is deterministic."""
        if node.node_type == NodeType.FUNCTION and node.function_name:
            func_info = self.function_registry.get_function(node.function_name)
            if func_info and not func_info.is_deterministic:
                return False

        # Check child nodes
        for child in [
            node.left,
            node.right,
            node.operand,
            node.condition,
            node.then_branch,
            node.else_branch,
        ]:
            if child and not self._is_deterministic(child):
                return False

        for child in node.arguments:
            if not self._is_deterministic(child):
                return False

        return True

    def _calculate_confidence(self) -> float:
        """Calculate parsing confidence based on errors and warnings."""
        base_confidence = 1.0

        # Reduce confidence for errors and warnings
        base_confidence -= len(self.errors) * 0.3
        base_confidence -= len(self.warnings) * 0.1

        return max(0.0, base_confidence)

    def _count_nodes(self, node: ASTNode) -> int:
        """Count total nodes in AST."""
        count = 1

        for child in [
            node.left,
            node.right,
            node.operand,
            node.condition,
            node.then_branch,
            node.else_branch,
        ]:
            if child:
                count += self._count_nodes(child)

        for child in node.arguments:
            count += self._count_nodes(child)

        return count

    def _calculate_depth(self, node: ASTNode) -> int:
        """Calculate maximum depth of AST."""
        max_depth = 0

        for child in [
            node.left,
            node.right,
            node.operand,
            node.condition,
            node.then_branch,
            node.else_branch,
        ]:
            if child:
                max_depth = max(max_depth, self._calculate_depth(child))

        for child in node.arguments:
            max_depth = max(max_depth, self._calculate_depth(child))

        return max_depth + 1

    def _count_functions(self, node: ASTNode) -> int:
        """Count function calls in AST."""
        count = 1 if node.node_type == NodeType.FUNCTION else 0

        for child in [
            node.left,
            node.right,
            node.operand,
            node.condition,
            node.then_branch,
            node.else_branch,
        ]:
            if child:
                count += self._count_functions(child)

        for child in node.arguments:
            count += self._count_functions(child)

        return count

    def _count_conditionals(self, node: ASTNode) -> int:
        """Count conditional statements in AST."""
        count = 1 if node.node_type in [NodeType.CONDITIONAL, NodeType.CASE] else 0

        for child in [
            node.left,
            node.right,
            node.operand,
            node.condition,
            node.then_branch,
            node.else_branch,
        ]:
            if child:
                count += self._count_conditionals(child)

        for child in node.arguments:
            count += self._count_conditionals(child)

        return count
