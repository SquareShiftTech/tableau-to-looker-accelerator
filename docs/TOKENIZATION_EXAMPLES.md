# Tokenization and Parsing Examples

## Complete Formula Examples

### Example 1: Simple Field Reference

**Formula:** `[adult]`

**Tokenization:**
```python
# Input: "[adult]"
# Pattern: r"\[([^\]]+)\]" matches
# Captured group: "adult"

Tokens: [
    Token(type=FIELD_REF, value="adult", position=0)
    Token(type=EOF, value="", position=7)
]
```

**AST Construction:**
```python
# parse_primary() encounters FIELD_REF token
ASTNode(
    node_type=NodeType.FIELD_REF,
    field_name="adult",           # normalized: lowercase
    original_name="[adult]"       # preserved original format
)
```

**Final JSON:**
```json
{
  "name": "adult_calculation",
  "calculation": {
    "original_formula": "[adult]",
    "ast": {
      "node_type": "field_ref",
      "field_name": "adult",
      "original_name": "[adult]"
    },
    "dependencies": ["adult"],
    "complexity": "simple"
  }
}
```

### Example 2: Arithmetic Expression

**Formula:** `[budget] + [revenue] * 0.1`

**Tokenization:**
```python
# Input: "[budget] + [revenue] * 0.1"
# Position:  01234567890123456789012345

Tokens: [
    Token(type=FIELD_REF, value="budget", position=0),
    Token(type=PLUS, value="+", position=9),
    Token(type=FIELD_REF, value="revenue", position=11),
    Token(type=MULTIPLY, value="*", position=21),
    Token(type=REAL, value="0.1", position=23),
    Token(type=EOF, value="", position=26)
]
```

**AST Construction (respecting precedence):**
```python
# * has higher precedence than +
# parse_term() handles +, parse_factor() handles *
# Result: budget + (revenue * 0.1)

ASTNode(
    node_type=NodeType.ARITHMETIC,
    operator="+",
    left=ASTNode(
        node_type=NodeType.FIELD_REF,
        field_name="budget"
    ),
    right=ASTNode(
        node_type=NodeType.ARITHMETIC,
        operator="*",
        left=ASTNode(
            node_type=NodeType.FIELD_REF,
            field_name="revenue"
        ),
        right=ASTNode(
            node_type=NodeType.LITERAL,
            value=0.1,
            data_type=DataType.REAL
        )
    )
)
```

### Example 3: Conditional Expression

**Formula:** `IF [adult] THEN "Adult" ELSE "Minor" END`

**Tokenization:**
```python
# Input: 'IF [adult] THEN "Adult" ELSE "Minor" END'
# Note: String quotes are removed during tokenization

Tokens: [
    Token(type=IF, value="IF", position=0),
    Token(type=FIELD_REF, value="adult", position=3),
    Token(type=THEN, value="THEN", position=10),
    Token(type=STRING, value="Adult", position=15),      # quotes removed
    Token(type=ELSE, value="ELSE", position=23),
    Token(type=STRING, value="Minor", position=28),      # quotes removed
    Token(type=END, value="END", position=36),
    Token(type=EOF, value="", position=39)
]
```

**AST Construction:**
```python
# parse_primary() encounters IF → calls parse_if_statement()

ASTNode(
    node_type=NodeType.CONDITIONAL,
    condition=ASTNode(
        node_type=NodeType.FIELD_REF,
        field_name="adult"
    ),
    then_branch=ASTNode(
        node_type=NodeType.LITERAL,
        value="Adult",
        data_type=DataType.STRING
    ),
    else_branch=ASTNode(
        node_type=NodeType.LITERAL,
        value="Minor",
        data_type=DataType.STRING
    )
)
```

### Example 4: Function Call

**Formula:** `SUM([budget] + [revenue])`

**Tokenization:**
```python
# Input: "SUM([budget] + [revenue])"

Tokens: [
    Token(type=IDENTIFIER, value="SUM", position=0),
    Token(type=LEFT_PAREN, value="(", position=3),
    Token(type=FIELD_REF, value="budget", position=4),
    Token(type=PLUS, value="+", position=13),
    Token(type=FIELD_REF, value="revenue", position=15),
    Token(type=RIGHT_PAREN, value=")", position=25),
    Token(type=EOF, value="", position=26)
]
```

**AST Construction:**
```python
# parse_primary() encounters IDENTIFIER followed by LEFT_PAREN
# → calls parse_function_call()

ASTNode(
    node_type=NodeType.FUNCTION,
    function_name="SUM",
    arguments=[
        ASTNode(
            node_type=NodeType.ARITHMETIC,
            operator="+",
            left=ASTNode(node_type=NodeType.FIELD_REF, field_name="budget"),
            right=ASTNode(node_type=NodeType.FIELD_REF, field_name="revenue")
        )
    ]
)
```

### Example 5: Complex Nested Expression

**Formula:** `IF SUM([budget]) > 1000000 THEN "Blockbuster" ELSE "Independent" END`

**Tokenization:**
```python
Tokens: [
    Token(type=IF, value="IF", position=0),
    Token(type=IDENTIFIER, value="SUM", position=3),
    Token(type=LEFT_PAREN, value="(", position=6),
    Token(type=FIELD_REF, value="budget", position=7),
    Token(type=RIGHT_PAREN, value=")", position=15),
    Token(type=GREATER_THAN, value=">", position=17),
    Token(type=INTEGER, value="1000000", position=19),
    Token(type=THEN, value="THEN", position=27),
    Token(type=STRING, value="Blockbuster", position=32),
    Token(type=ELSE, value="ELSE", position=46),
    Token(type=STRING, value="Independent", position=51),
    Token(type=END, value="END", position=65),
    Token(type=EOF, value="", position=68)
]
```

**AST Construction:**
```python
ASTNode(
    node_type=NodeType.CONDITIONAL,
    condition=ASTNode(
        node_type=NodeType.COMPARISON,
        operator=">",
        left=ASTNode(
            node_type=NodeType.FUNCTION,
            function_name="SUM",
            arguments=[
                ASTNode(node_type=NodeType.FIELD_REF, field_name="budget")
            ]
        ),
        right=ASTNode(
            node_type=NodeType.LITERAL,
            value=1000000,
            data_type=DataType.INTEGER
        )
    ),
    then_branch=ASTNode(
        node_type=NodeType.LITERAL,
        value="Blockbuster",
        data_type=DataType.STRING
    ),
    else_branch=ASTNode(
        node_type=NodeType.LITERAL,
        value="Independent",
        data_type=DataType.STRING
    )
)
```

## Pattern Matching Examples

### String Patterns

```python
# Pattern: r'"([^"\\]|\\.)*"'

# ✅ Matches:
'"Hello World"'           → STRING("Hello World")
'"Text with \\"quotes\\""' → STRING('Text with "quotes"')
'""'                      → STRING("")
'"[Field Reference]"'     → STRING("[Field Reference]")  # Protected!

# ❌ Doesn't match:
"'Single quotes'"         → Not a double-quoted string
'"Unclosed string'        → Missing closing quote
```

### Field Reference Patterns

```python
# Pattern: r"\[([^\]]+)\]"

# ✅ Matches:
"[adult]"                 → FIELD_REF("adult")
"[Movie Title]"           → FIELD_REF("Movie Title")  # Spaces preserved
"[budget_2023]"           → FIELD_REF("budget_2023")

# ❌ Doesn't match:
"[]"                      → Empty field name (+ requires at least one char)
"[unclosed"               → Missing closing bracket
"adult"                   → No brackets
```

### Number Patterns

```python
# Patterns: r"\d+\.\d+" (REAL), r"\d+" (INTEGER)

# ✅ REAL matches:
"123.45"                  → REAL(123.45)
"0.5"                     → REAL(0.5)
"999.999"                 → REAL(999.999)

# ✅ INTEGER matches:
"123"                     → INTEGER(123)
"0"                       → INTEGER(0)

# ❌ Edge cases:
".5"                      → DOT(".") + INTEGER(5)  # No leading digit
"123."                    → INTEGER(123) + DOT(".") # No trailing digit
```

### Operator Patterns

```python
# Multi-character operators BEFORE single-character

# ✅ Correct tokenization:
"<="                      → LESS_EQUAL("<=")
"!="                      → NOT_EQUAL("!=")
"<>"                      → NOT_EQUAL("<>")
"<"                       → LESS_THAN("<")

# Why order matters:
# If single-char patterns came first:
# "<=" would become LESS_THAN("<") + EQUAL("=") ❌
```

### Keyword Patterns

```python
# Pattern: r"(?i)\bIF\b" (case-insensitive with word boundaries)

# ✅ Matches:
"IF"                      → IF("IF")
"if"                      → IF("if")
"If"                      → IF("If")
"IF [adult]"              → IF("IF") + FIELD_REF("adult")
"(IF"                     → LEFT_PAREN("(") + IF("IF")

# ❌ Doesn't match:
"IFFY"                    → IDENTIFIER("IFFY")  # Not complete word
"DIFF"                    → IDENTIFIER("DIFF")  # IF not at word boundary
```

## Edge Cases and Special Handling

### Case 1: Keywords Inside Strings
```python
# Input: '"IF statement text"'
# String pattern matches first, protects contents

Tokens: [STRING("IF statement text")]
# IF keyword pattern never gets a chance to match inside the string ✓
```

### Case 2: Field Names That Look Like Keywords
```python
# Input: '[IF]'
# Field reference pattern matches first

Tokens: [FIELD_REF("IF")]
# Creates a field reference named "IF", not an IF keyword ✓
```

### Case 3: Negative Numbers
```python
# Input: '-123'
# Tokenizes as separate MINUS and INTEGER tokens

Tokens: [MINUS("-"), INTEGER(123)]
# Parser handles this as unary negation during AST construction
```

### Case 4: Nested Parentheses
```python
# Input: 'SUM(AVG([budget]) + MAX([revenue]))'

Tokens: [
    IDENTIFIER("SUM"), LEFT_PAREN("("),
    IDENTIFIER("AVG"), LEFT_PAREN("("), FIELD_REF("budget"), RIGHT_PAREN(")"),
    PLUS("+"),
    IDENTIFIER("MAX"), LEFT_PAREN("("), FIELD_REF("revenue"), RIGHT_PAREN(")"),
    RIGHT_PAREN(")")
]
# Parser handles nesting through recursive calls
```

### Case 5: Mixed Quotes
```python
# Input: '"Text with \'single quotes\' inside"'

Tokens: [STRING("Text with 'single quotes' inside")]
# Double-quote pattern captures everything including single quotes ✓
```

## Error Handling Examples

### Tokenization Errors

```python
# Input with unknown character: '[field] @ value'

Tokens: [
    FIELD_REF("field"),
    UNKNOWN("@"),        # Unrecognized character
    IDENTIFIER("value")
]
# Parser can still process known tokens, logs warning for unknown
```

### Parsing Errors

```python
# Input with syntax error: 'IF [adult] THEN'  (missing ELSE/END)

Tokens: [IF, FIELD_REF("adult"), THEN, EOF]

# parse_if_statement() calls:
condition = parse_expression()     # ✓ Gets field reference
consume(THEN)                     # ✓ Consumes THEN
then_branch = parse_expression()  # ❌ Gets EOF, creates error
# Parser creates error but attempts to continue
```

## Best Practices from Examples

### 1. Pattern Specificity
- Containers first: `"strings"`, `[fields]`
- Keywords before identifiers: `IF` before generic words
- Long operators before short: `<=` before `<`

### 2. Error Recovery
- Continue parsing after errors to find multiple issues
- Create fallback AST nodes rather than failing completely
- Collect all errors for comprehensive feedback

### 3. Token Value Processing
- Remove container characters: `"text"` → `text`
- Preserve original formatting in metadata
- Normalize case for keywords but preserve user input

### 4. AST Construction
- Respect operator precedence during parsing
- Create immutable AST nodes
- Include source location information for debugging

### 5. Testing Strategy
- Test edge cases: empty strings, special characters
- Verify pattern order with conflict detection
- Use parameterized tests for multiple similar cases

These examples demonstrate the complete flow from raw formula text to structured AST, showing how proper tokenization and parsing patterns enable robust formula processing.
