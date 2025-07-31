# Tokenization and Parsing Documentation

## Overview

The Tableau Formula Parser uses a **3-phase pipeline** to convert Tableau formula strings into Abstract Syntax Trees (ASTs):

1. **Tokenization**: Break formula strings into tokens using regex patterns
2. **Token Classification**: Identify token types (keywords, literals, field references, etc.)
3. **AST Construction**: Build tree structure using recursive descent parsing

## Architecture

```
Raw Formula → Tokenizer → Parser → AST → CalculatedField → JSON
"[adult]"   → FIELD_REF → ASTNode → Tree → Metadata → Output
```

### Key Components

- **FormulaLexer** (`src/tableau_to_looker_parser/converters/formula_parser.py:33`): Tokenization engine
- **FormulaParser** (`src/tableau_to_looker_parser/converters/formula_parser.py:167`): Recursive descent parser
- **ASTNode** (`src/tableau_to_looker_parser/models/ast_schema.py:51`): Unified tree node structure

## Phase 1: Tokenization

### How It Works

The lexer scans the formula character-by-character, trying regex patterns **in order** until one matches:

```python
# Example: "IF [adult] THEN 'Yes' END"
position = 0: "IF" matches r"(?i)\bIF\b" → Token(type=IF, value="IF")
position = 3: "[adult]" matches r"\[([^\]]+)\]" → Token(type=FIELD_REF, value="adult")
position = 10: "THEN" matches r"(?i)\bTHEN\b" → Token(type=THEN, value="THEN")
# ... and so on
```

### Token Types

| Token Type | Pattern | Description | Example |
|------------|---------|-------------|---------|
| `STRING` | `r'"([^"\\]|\\.)*"'` | Quoted string literals | `"Hello"` |
| `FIELD_REF` | `r"\[([^\]]+)\]"` | Tableau field references | `[adult]`, `[Movie Title]` |
| `INTEGER` | `r"\d+"` | Integer numbers | `123`, `0` |
| `REAL` | `r"\d+\.\d+"` | Decimal numbers | `123.45`, `0.5` |
| `IF/THEN/ELSE` | `r"(?i)\b(IF\|THEN\|ELSE)\b"` | Control flow keywords | `IF`, `then`, `ElSe` |
| `IDENTIFIER` | `r"[a-zA-Z_][a-zA-Z0-9_]*"` | Function names, variables | `SUM`, `COUNT` |
| `PLUS/MINUS` | `r"[+-]"` | Arithmetic operators | `+`, `-` |
| `COMPARISON` | `r"(<=\|>=\|!=\|<>\|[<>=])"` | Comparison operators | `<=`, `!=`, `=` |

### Special Token Processing

Some tokens get special processing after matching:

```python
if token_type == TokenType.FIELD_REF:
    # Remove brackets: "[adult]" → "adult"
    value = match.group(1)

elif token_type == TokenType.STRING:
    # Remove quotes: '"Hello"' → "Hello"
    value = value[1:-1]

elif token_type == TokenType.BOOLEAN:
    # Normalize case: "true" → "TRUE"
    value = value.upper()
```

## Phase 2: Token Classification

Tokens are classified into semantic categories that guide AST construction:

### Classification Rules

1. **Literals**: `STRING`, `INTEGER`, `REAL`, `BOOLEAN`, `NULL`
2. **References**: `FIELD_REF`, `IDENTIFIER`
3. **Operators**: `PLUS`, `MINUS`, `MULTIPLY`, `DIVIDE`, `COMPARISON`, `LOGICAL`
4. **Control Flow**: `IF`, `THEN`, `ELSE`, `END`, `CASE`, `WHEN`
5. **Structure**: `LEFT_PAREN`, `RIGHT_PAREN`, `COMMA`

## Phase 3: AST Construction

### Recursive Descent Parsing

The parser uses **precedence-driven recursive descent** with these levels:

```python
# Precedence levels (higher number = higher precedence)
parse_expression()      # Entry point
  parse_or_expression()    # OR (precedence 1)
    parse_and_expression()   # AND (precedence 2)
      parse_equality()         # = != (precedence 3)
        parse_comparison()       # < <= > >= (precedence 3)
          parse_term()             # + - (precedence 4)
            parse_factor()           # * / % (precedence 5)
              parse_unary()            # NOT - (unary)
                parse_power()            # ^ (precedence 6)
                  parse_primary()          # literals, fields, functions, IF
```

### Token Type → AST Node Mapping

```python
# In parse_primary():
TokenType.STRING     → ASTNode(node_type=LITERAL, data_type=STRING)
TokenType.INTEGER    → ASTNode(node_type=LITERAL, data_type=INTEGER)
TokenType.FIELD_REF  → ASTNode(node_type=FIELD_REF)
TokenType.IF         → ASTNode(node_type=CONDITIONAL)
TokenType.IDENTIFIER → ASTNode(node_type=FUNCTION) # when followed by (

# In binary expression methods:
TokenType.PLUS       → ASTNode(node_type=ARITHMETIC, operator="+")
TokenType.EQUAL      → ASTNode(node_type=COMPARISON, operator="=")
TokenType.AND        → ASTNode(node_type=LOGICAL, operator="AND")
```

## Complete Example Walkthrough

### Formula: `IF [adult] THEN "Yes" ELSE "No" END`

**Phase 1: Tokenization**
```
Input: "IF [adult] THEN \"Yes\" ELSE \"No\" END"
Tokens: [IF, FIELD_REF("adult"), THEN, STRING("Yes"), ELSE, STRING("No"), END, EOF]
```

**Phase 2: Classification**
```
IF         → Control flow keyword
FIELD_REF  → Field reference
THEN       → Control flow keyword
STRING     → Literal value
ELSE       → Control flow keyword
STRING     → Literal value
END        → Control flow keyword
```

**Phase 3: AST Construction**
```python
# parse_expression() → ... → parse_primary()
# Encounters IF token → calls parse_if_statement()

ASTNode(
    node_type=NodeType.CONDITIONAL,
    condition=ASTNode(
        node_type=NodeType.FIELD_REF,
        field_name="adult",
        original_name="[adult]"
    ),
    then_branch=ASTNode(
        node_type=NodeType.LITERAL,
        value="Yes",
        data_type=DataType.STRING
    ),
    else_branch=ASTNode(
        node_type=NodeType.LITERAL,
        value="No",
        data_type=DataType.STRING
    )
)
```

## Pattern Ordering Rules

⚠️ **CRITICAL**: Pattern order determines tokenization correctness!

### Rule 1: Most Specific → Most General
```python
# ✅ Correct:
(r'"([^"\\]|\\.)*"', TokenType.STRING),           # Very specific
(r"(?i)\bIF\b", TokenType.IF),                    # Specific keyword
(r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER) # General catch-all
```

### Rule 2: Longer Patterns → Shorter Patterns
```python
# ✅ Correct:
(r"<=", TokenType.LESS_EQUAL),    # 2 characters
(r"<", TokenType.LESS_THAN),      # 1 character
```

### Rule 3: Container Patterns First
```python
# ✅ Correct:
(r'"([^"\\]|\\.)*"', TokenType.STRING),    # Protects string contents
(r"\[([^\]]+)\]", TokenType.FIELD_REF),    # Protects field names
# Then other patterns...
```

### Rule 4: Keywords Before Identifiers
```python
# ✅ Correct:
(r"(?i)\bIF\b", TokenType.IF),                    # Keyword first
(r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER) # Identifier last
```

## Error Handling

### Tokenization Errors
- **Unknown characters**: Create `UNKNOWN` tokens for debugging
- **Unclosed strings**: Regex fails to match, creates `UNKNOWN` tokens
- **Invalid patterns**: Logged as warnings, parsing continues

### Parsing Errors
- **Unexpected tokens**: Added to error list, parsing attempts to continue
- **Missing tokens**: `consume()` method adds error but doesn't stop parsing
- **Syntax errors**: Collected in `self.errors` list, returned in parse result

### Recovery Strategies
1. **Fallback AST nodes**: Create `LITERAL(null)` nodes for unparseable sections
2. **Error collection**: Continue parsing to find multiple errors
3. **Confidence scoring**: Lower confidence for formulas with errors
4. **Graceful degradation**: Return partial AST rather than failing completely

## Extension Points

### Adding New Token Types
1. Add pattern to `TOKEN_PATTERNS` in correct position
2. Add corresponding `TokenType` enum value
3. Update parser to handle new token in appropriate method

### Adding New AST Node Types
1. Add to `NodeType` enum
2. Add fields to `ASTNode` class if needed
3. Update parser methods to create new node type
4. Update analysis methods (`_extract_dependencies`, etc.)

### Adding New Functions
1. Register in `FunctionRegistry`
2. Parser automatically handles as `IDENTIFIER` + `LEFT_PAREN`
3. Add return type and behavior metadata for analysis

## Best Practices

### For Token Pattern Design
- Use word boundaries (`\b`) for keywords
- Make string/container patterns greedy
- Test patterns with edge cases
- Document regex complexity

### For Parser Extension
- Follow precedence hierarchy
- Use descriptive method names
- Add comprehensive error messages
- Test with malformed input

### For AST Design
- Keep nodes immutable after creation
- Use composition over inheritance
- Include source location for debugging
- Design for serialization/deserialization

## Performance Considerations

### Tokenization
- Patterns are compiled once at lexer initialization
- Linear scan with backtracking only on pattern failure
- Most formulas tokenize in microseconds

### Parsing
- Recursive descent with limited lookahead
- No backtracking in grammar
- AST construction is single-pass
- Memory usage scales with formula complexity

### Optimization Tips
- Put most common patterns first (after specificity rules)
- Minimize regex complexity where possible
- Cache compiled patterns
- Use string interning for common values

## Debugging Guide

### Tokenization Issues
1. Print token stream: `FormulaLexer().tokenize(formula)`
2. Check pattern order for conflicts
3. Test individual patterns in isolation
4. Look for `UNKNOWN` tokens

### Parsing Issues
1. Enable debug logging: `logging.getLogger('formula_parser').setLevel(DEBUG)`
2. Examine parse error messages
3. Check token consumption in debugger
4. Validate AST structure manually

### Common Problems
- **Keywords parsed as identifiers**: Check pattern order
- **Operators split incorrectly**: Check multi-char vs single-char order
- **Strings not recognized**: Check quote handling and escaping
- **Field references broken**: Check bracket pattern and extraction logic
