# Pattern Ordering Guide

## Why Pattern Order Matters

The lexer tries regex patterns **sequentially** - **first match wins**! This makes pattern order absolutely critical for correct tokenization.

## The Ordering Problem

### ❌ Wrong Order Example: Keywords vs Identifiers
```python
# WRONG - Generic pattern first
TOKEN_PATTERNS = [
    (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER),  # Too general
    (r"(?i)\bIF\b", TokenType.IF),                      # Never reached
]

# Input: "IF [adult] THEN 'Yes' END"
# Result: [IDENTIFIER("IF"), FIELD_REF("adult"), IDENTIFIER("THEN"), ...]
# Parser fails - expects IF token but gets IDENTIFIER!
```

### ✅ Correct Order Example
```python
# CORRECT - Specific patterns first
TOKEN_PATTERNS = [
    (r"(?i)\bIF\b", TokenType.IF),                      # Specific first
    (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER), # General last
]

# Input: "IF [adult] THEN 'Yes' END"
# Result: [IF("IF"), FIELD_REF("adult"), THEN("THEN"), ...]
# Parser works correctly!
```

## The Four Golden Rules

### Rule 1: Container Patterns First
**Containers protect their contents from other patterns**

```python
# ✅ CORRECT ORDER
TOKEN_PATTERNS = [
    # Containers first - protect contents
    (r'"([^"\\]|\\.)*"', TokenType.STRING),     # "text with [brackets]"
    (r"'([^'\\]|\\.)*'", TokenType.STRING),     # 'text with IF keywords'
    (r"\[([^\]]+)\]", TokenType.FIELD_REF),     # [Field Name]

    # Then other patterns...
    (r"(?i)\bIF\b", TokenType.IF),
    (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER),
]
```

**Why this works:**
- `"IF [field]"` becomes `STRING("IF [field]")` - contents protected
- `[SUM]` becomes `FIELD_REF("SUM")` - not confused with SUM function
- Containers consume their entire contents as single tokens

### Rule 2: Longer Patterns Before Shorter Patterns
**Prevents shorter patterns from stealing parts of longer ones**

```python
# ❌ WRONG - Short patterns steal characters
TOKEN_PATTERNS = [
    (r"<", TokenType.LESS_THAN),      # Matches first "<" only
    (r"=", TokenType.EQUAL),          # Matches the "=" separately
    (r"<=", TokenType.LESS_EQUAL),    # Never reached!
]

# Input: "[budget] <= [revenue]"
# Result: [FIELD_REF("budget"), LESS_THAN("<"), EQUAL("="), FIELD_REF("revenue")]
# Wrong! Should be single LESS_EQUAL token

# ✅ CORRECT - Long patterns first
TOKEN_PATTERNS = [
    (r"!=|<>", TokenType.NOT_EQUAL),     # 2 characters
    (r"<=", TokenType.LESS_EQUAL),       # 2 characters
    (r">=", TokenType.GREATER_EQUAL),    # 2 characters
    (r"<", TokenType.LESS_THAN),         # 1 character
    (r">", TokenType.GREATER_THAN),      # 1 character
    (r"=", TokenType.EQUAL),             # 1 character
]
```

### Rule 3: Specific Patterns Before General Patterns
**Specific wins over generic**

```python
# ✅ CORRECT SPECIFICITY ORDER
TOKEN_PATTERNS = [
    # Most specific - exact matches
    (r"(?i)\bIF\b", TokenType.IF),
    (r"(?i)\bTHEN\b", TokenType.THEN),
    (r"(?i)\bELSE\b", TokenType.ELSE),
    (r"(?i)\bTRUE\b", TokenType.BOOLEAN),
    (r"(?i)\bFALSE\b", TokenType.BOOLEAN),

    # Less specific - number patterns
    (r"\d+\.\d+", TokenType.REAL),       # Specific number format
    (r"\d+", TokenType.INTEGER),         # General number format

    # Least specific - catch-all
    (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER),  # Any word
]
```

### Rule 4: Use Word Boundaries for Keywords
**Prevents partial word matches**

```python
# ❌ WRONG - No word boundaries
(r"(?i)IF", TokenType.IF)  # Matches "DIFF", "IFFY", etc.

# ✅ CORRECT - With word boundaries
(r"(?i)\bIF\b", TokenType.IF)  # Only matches complete "IF" words

# Examples:
"IF [adult]"    → IF token ✓
"IFFY behavior" → IDENTIFIER("IFFY") ✓ (not IF)
"DIFF value"    → IDENTIFIER("DIFF") ✓ (not IF)
```

## Complete Correct Pattern Order

```python
TOKEN_PATTERNS = [
    # 1. CONTAINERS (highest priority - protect contents)
    (r'"([^"\\]|\\.)*"', TokenType.STRING),      # "quoted strings"
    (r"'([^'\\]|\\.)*'", TokenType.STRING),      # 'quoted strings'
    (r"\[([^\]]+)\]", TokenType.FIELD_REF),      # [Field References]

    # 2. NUMBERS (before operators that might conflict)
    (r"\d+\.\d+", TokenType.REAL),               # 123.45 (before integer)
    (r"\d+", TokenType.INTEGER),                 # 123

    # 3. MULTI-CHARACTER OPERATORS (before single characters)
    (r"!=|<>", TokenType.NOT_EQUAL),             # != or <>
    (r"<=", TokenType.LESS_EQUAL),               # <=
    (r">=", TokenType.GREATER_EQUAL),            # >=

    # 4. SINGLE-CHARACTER OPERATORS
    (r"\+", TokenType.PLUS),                     # +
    (r"-", TokenType.MINUS),                     # -
    (r"\*", TokenType.MULTIPLY),                 # *
    (r"/", TokenType.DIVIDE),                    # /
    (r"%", TokenType.MODULO),                    # %
    (r"\^", TokenType.POWER),                    # ^
    (r"=", TokenType.EQUAL),                     # =
    (r"<", TokenType.LESS_THAN),                 # <
    (r">", TokenType.GREATER_THAN),              # >

    # 5. PUNCTUATION
    (r"\(", TokenType.LEFT_PAREN),               # (
    (r"\)", TokenType.RIGHT_PAREN),              # )
    (r",", TokenType.COMMA),                     # ,

    # 6. KEYWORDS (all specific keywords before generic identifiers)
    (r"(?i)\bIF\b", TokenType.IF),               # IF, if, If
    (r"(?i)\bTHEN\b", TokenType.THEN),           # THEN, then
    (r"(?i)\bELSE\b", TokenType.ELSE),           # ELSE, else
    (r"(?i)\bEND\b", TokenType.END),             # END, end
    (r"(?i)\bCASE\b", TokenType.CASE),           # CASE, case
    (r"(?i)\bWHEN\b", TokenType.WHEN),           # WHEN, when
    (r"(?i)\bAND\b", TokenType.AND),             # AND, and
    (r"(?i)\bOR\b", TokenType.OR),               # OR, or
    (r"(?i)\bNOT\b", TokenType.NOT),             # NOT, not
    (r"(?i)\bTRUE\b", TokenType.BOOLEAN),        # TRUE, true
    (r"(?i)\bFALSE\b", TokenType.BOOLEAN),       # FALSE, false
    (r"(?i)\bNULL\b", TokenType.NULL),           # NULL, null

    # 7. GENERIC IDENTIFIERS (lowest priority - catch-all)
    (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER),  # SUM, COUNT, myfield
]
```

## Common Ordering Mistakes

### Mistake 1: Generic Before Specific
```python
# ❌ DON'T DO THIS
TOKEN_PATTERNS = [
    (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER),  # Catches everything
    (r"(?i)\bIF\b", TokenType.IF),                      # Never reached
]
```

### Mistake 2: Short Before Long
```python
# ❌ DON'T DO THIS
TOKEN_PATTERNS = [
    (r"<", TokenType.LESS_THAN),        # Steals from <=
    (r"<=", TokenType.LESS_EQUAL),      # Never reached
]
```

### Mistake 3: Keywords Without Word Boundaries
```python
# ❌ DON'T DO THIS
(r"(?i)IF", TokenType.IF)  # Matches partial words like "DIFF"

# ✅ DO THIS
(r"(?i)\bIF\b", TokenType.IF)  # Only complete words
```

### Mistake 4: Containers After Content Patterns
```python
# ❌ DON'T DO THIS
TOKEN_PATTERNS = [
    (r"(?i)\bIF\b", TokenType.IF),           # Tries to match IF inside strings
    (r'"([^"\\]|\\.)*"', TokenType.STRING),  # String protection comes too late
]

# Input: "IF statement text"
# Result: [STRING("IF statement text")] ✗ - IF never matched inside string

# ✅ DO THIS - Strings first
TOKEN_PATTERNS = [
    (r'"([^"\\]|\\.)*"', TokenType.STRING),  # Protect string contents first
    (r"(?i)\bIF\b", TokenType.IF),           # Then try keywords
]
```

## Testing Pattern Order

### Quick Tests for New Patterns

```python
# Test cases for pattern ordering
test_cases = [
    # Keywords vs identifiers
    ("IF", [("IF", TokenType.IF)]),
    ("SUM", [("SUM", TokenType.IDENTIFIER)]),

    # Multi-char operators
    ("<=", [("<=", TokenType.LESS_EQUAL)]),
    ("<", [("<", TokenType.LESS_THAN)]),

    # Strings vs other patterns
    ('"IF [field]"', [('"IF [field]"', TokenType.STRING)]),
    ('[field]', [("field", TokenType.FIELD_REF)]),

    # Numbers
    ("123.45", [("123.45", TokenType.REAL)]),
    ("123", [("123", TokenType.INTEGER)]),
]

def test_tokenization_order():
    lexer = FormulaLexer()
    for input_str, expected in test_cases:
        tokens = lexer.tokenize(input_str)
        # Verify tokens match expected types and values
        assert len(tokens) == len(expected) + 1  # +1 for EOF
        for i, (exp_value, exp_type) in enumerate(expected):
            assert tokens[i].value == exp_value
            assert tokens[i].type == exp_type
```

## Pattern Order Validation

### Automated Checks

```python
def validate_pattern_order(patterns):
    """Check for common pattern ordering issues."""
    issues = []

    # Check for generic patterns before specific ones
    identifier_index = None
    for i, (pattern, token_type) in enumerate(patterns):
        if token_type == TokenType.IDENTIFIER:
            identifier_index = i
            break

    if identifier_index is not None:
        for i, (pattern, token_type) in enumerate(patterns[identifier_index:]):
            if token_type in [TokenType.IF, TokenType.THEN, TokenType.ELSE]:
                issues.append(f"Keyword {token_type} after IDENTIFIER at position {i}")

    # Check for short patterns before long ones
    for i in range(len(patterns) - 1):
        current_pattern, current_type = patterns[i]
        next_pattern, next_type = patterns[i + 1]

        if (current_type in [TokenType.LESS_THAN, TokenType.EQUAL] and
            next_type == TokenType.LESS_EQUAL):
            issues.append(f"Short operator {current_type} before long operator {next_type}")

    return issues

# Usage
issues = validate_pattern_order(TOKEN_PATTERNS)
if issues:
    print("Pattern ordering issues found:")
    for issue in issues:
        print(f"  - {issue}")
```

## Debugging Pattern Conflicts

### When Tokenization Goes Wrong

1. **Print the token stream**:
   ```python
   tokens = FormulaLexer().tokenize("your formula here")
   for token in tokens:
       print(f"{token.type}: '{token.value}' at position {token.position}")
   ```

2. **Test individual patterns**:
   ```python
   import re
   pattern = r"(?i)\bIF\b"
   test_string = "IF [adult]"
   match = re.match(pattern, test_string)
   if match:
       print(f"Pattern matched: '{match.group(0)}'")
   ```

3. **Check pattern order conflicts**:
   ```python
   # Test if a more general pattern is stealing matches
   patterns_to_test = [
       (r"[a-zA-Z_][a-zA-Z0-9_]*", "IDENTIFIER"),
       (r"(?i)\bIF\b", "IF"),
   ]

   test_input = "IF"
   for pattern, name in patterns_to_test:
       if re.match(pattern, test_input):
           print(f"'{test_input}' matches {name}")
           break  # First match wins!
   ```

## Summary

Pattern order is the **foundation of correct tokenization**. Follow these principles:

1. **Containers first** - protect quoted strings and bracketed field names
2. **Specific before general** - keywords before generic identifiers
3. **Long before short** - multi-character operators before single characters
4. **Use word boundaries** - prevent partial matches for keywords

Get the order wrong, and the entire parsing pipeline fails. Get it right, and tokenization becomes reliable and predictable.
