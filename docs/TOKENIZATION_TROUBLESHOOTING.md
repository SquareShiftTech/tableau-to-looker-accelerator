# Tokenization Troubleshooting Guide

## Quick Diagnostics

### 1. Print Token Stream
```python
from tableau_to_looker_parser.converters.formula_parser import FormulaLexer

def debug_tokenization(formula):
    lexer = FormulaLexer()
    tokens = lexer.tokenize(formula)

    print(f"Formula: '{formula}'")
    print("Tokens:")
    for i, token in enumerate(tokens):
        print(f"  {i}: {token.type.value:15} '{token.value}' at pos {token.position}")
    return tokens

# Usage
tokens = debug_tokenization("IF [adult] THEN 'Yes' ELSE 'No' END")
```

### 2. Test Individual Patterns
```python
import re

def test_pattern(pattern, test_string, description=""):
    match = re.match(pattern, test_string)
    if match:
        print(f"✓ {description}")
        print(f"  Pattern: {pattern}")
        print(f"  Input: '{test_string}'")
        print(f"  Match: '{match.group(0)}'")
        if match.groups():
            print(f"  Groups: {match.groups()}")
    else:
        print(f"✗ {description}")
        print(f"  Pattern: {pattern}")
        print(f"  Input: '{test_string}' - NO MATCH")

# Usage
test_pattern(r"\[([^\]]+)\]", "[adult]", "Field reference pattern")
test_pattern(r"(?i)\bIF\b", "IF", "IF keyword pattern")
```

## Common Problems & Solutions

### Problem 1: Keywords Parsed as Identifiers

**Symptoms:**
```python
# Input: "IF [adult] THEN 'Yes' END"
# Wrong output: [IDENTIFIER("IF"), FIELD_REF("adult"), IDENTIFIER("THEN"), ...]
# Parser error: Expected IF token but got IDENTIFIER
```

**Diagnosis:**
```python
def diagnose_keyword_issue(keyword):
    patterns = [
        (r"[a-zA-Z_][a-zA-Z0-9_]*", "IDENTIFIER"),
        (r"(?i)\bIF\b", "IF"),
    ]

    for pattern, name in patterns:
        if re.match(pattern, keyword):
            print(f"'{keyword}' matched by {name} pattern")
            break
```

**Root Cause:**
Generic identifier pattern comes before specific keyword patterns.

**Solution:**
```python
# ✅ Move keywords BEFORE identifier pattern
TOKEN_PATTERNS = [
    (r"(?i)\bIF\b", TokenType.IF),                      # Specific first
    (r"(?i)\bTHEN\b", TokenType.THEN),
    (r"(?i)\bELSE\b", TokenType.ELSE),
    (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER), # General last
]
```

### Problem 2: Multi-Character Operators Split

**Symptoms:**
```python
# Input: "[budget] <= [revenue]"
# Wrong output: [FIELD_REF("budget"), LESS_THAN("<"), EQUAL("="), FIELD_REF("revenue")]
# Should be: [FIELD_REF("budget"), LESS_EQUAL("<="), FIELD_REF("revenue")]
```

**Diagnosis:**
```python
def diagnose_operator_splitting():
    test_input = "<="
    patterns = [
        (r"<", "LESS_THAN"),
        (r"=", "EQUAL"),
        (r"<=", "LESS_EQUAL"),
    ]

    # Simulate lexer behavior
    position = 0
    while position < len(test_input):
        for pattern, name in patterns:
            match = re.match(pattern, test_input[position:])
            if match:
                print(f"Position {position}: '{match.group(0)}' → {name}")
                position += len(match.group(0))
                break
```

**Root Cause:**
Single-character operators come before multi-character ones.

**Solution:**
```python
# ✅ Move multi-character operators FIRST
TOKEN_PATTERNS = [
    (r"!=|<>", TokenType.NOT_EQUAL),     # 2 chars first
    (r"<=", TokenType.LESS_EQUAL),       # 2 chars first
    (r">=", TokenType.GREATER_EQUAL),    # 2 chars first
    (r"<", TokenType.LESS_THAN),         # 1 char second
    (r">", TokenType.GREATER_THAN),      # 1 char second
    (r"=", TokenType.EQUAL),             # 1 char second
]
```

### Problem 3: Field References Not Recognized

**Symptoms:**
```python
# Input: "[Movie Title]"
# Wrong output: [LEFT_BRACKET("["), IDENTIFIER("Movie"), IDENTIFIER("Title"), RIGHT_BRACKET("]")]
# Should be: [FIELD_REF("Movie Title")]
```

**Diagnosis:**
```python
def diagnose_field_ref_issue(field_ref):
    print(f"Testing field reference: '{field_ref}'")

    # Test the field reference pattern
    field_pattern = r"\[([^\]]+)\]"
    match = re.match(field_pattern, field_ref)
    if match:
        print(f"✓ Field pattern matches: '{match.group(0)}'")
        print(f"  Captured field name: '{match.group(1)}'")
    else:
        print("✗ Field pattern does not match")

    # Test what other patterns might match first
    competing_patterns = [
        (r"\[", "LEFT_BRACKET"),
        (r"\]", "RIGHT_BRACKET"),
        (r"[a-zA-Z_][a-zA-Z0-9_]*", "IDENTIFIER"),
    ]

    for pattern, name in competing_patterns:
        if re.match(pattern, field_ref):
            print(f"⚠️  Competing pattern {name} also matches!")
```

**Root Cause:**
Field reference pattern comes after bracket or identifier patterns.

**Solution:**
```python
# ✅ Move field references BEFORE competing patterns
TOKEN_PATTERNS = [
    (r"\[([^\]]+)\]", TokenType.FIELD_REF),      # Specific field syntax first
    (r"\[", TokenType.LEFT_BRACKET),             # Generic bracket second
    (r"\]", TokenType.RIGHT_BRACKET),
    (r"[a-zA-Z_][a-zA-Z0-9_]*", TokenType.IDENTIFIER),
]
```

### Problem 4: Strings Not Recognized

**Symptoms:**
```python
# Input: '"Hello [world]"'
# Wrong output: [QUOTE('"'), IDENTIFIER("Hello"), FIELD_REF("world"), QUOTE('"')]
# Should be: [STRING("Hello [world]")]
```

**Diagnosis:**
```python
def diagnose_string_issue(string_literal):
    print(f"Testing string: {string_literal}")

    # Test string pattern
    string_pattern = r'"([^"\\]|\\.)*"'
    match = re.match(string_pattern, string_literal)
    if match:
        print(f"✓ String pattern matches: '{match.group(0)}'")
        print(f"  Content: '{match.group(1)}'")
    else:
        print("✗ String pattern does not match")

        # Test what might be wrong
        if not string_literal.startswith('"'):
            print("  Missing opening quote")
        elif not string_literal.endswith('"'):
            print("  Missing closing quote")
        else:
            print("  Pattern might be incorrect")
```

**Root Cause:**
String pattern comes after other patterns that match quotes or contents.

**Solution:**
```python
# ✅ Move strings to VERY TOP - protect their contents
TOKEN_PATTERNS = [
    (r'"([^"\\]|\\.)*"', TokenType.STRING),      # Strings first
    (r"'([^'\\]|\\.)*'", TokenType.STRING),      # All quote types first
    (r"\[([^\]]+)\]", TokenType.FIELD_REF),      # Then other patterns
    # ... rest of patterns
]
```

### Problem 5: Numbers Not Parsed Correctly

**Symptoms:**
```python
# Input: "123.45"
# Wrong output: [INTEGER(123), DOT("."), INTEGER(45)]
# Should be: [REAL(123.45)]
```

**Diagnosis:**
```python
def diagnose_number_issue(number_str):
    patterns = [
        (r"\d+", "INTEGER"),
        (r"\d+\.\d+", "REAL"),
        (r"\.", "DOT"),
    ]

    print(f"Testing number: '{number_str}'")

    # Test in current order
    for pattern, name in patterns:
        match = re.match(pattern, number_str)
        if match:
            print(f"First match: {name} → '{match.group(0)}'")
            remaining = number_str[len(match.group(0)):]
            if remaining:
                print(f"Remaining: '{remaining}'")
            break
```

**Root Cause:**
Integer pattern comes before real (decimal) pattern.

**Solution:**
```python
# ✅ More specific patterns first
TOKEN_PATTERNS = [
    (r"\d+\.\d+", TokenType.REAL),      # Decimal numbers first
    (r"\d+", TokenType.INTEGER),        # Integers second
    (r"\.", TokenType.DOT),             # Dot operator last
]
```

## Advanced Debugging Techniques

### Pattern Conflict Detector
```python
def find_pattern_conflicts(patterns, test_cases):
    """Find patterns that might conflict with each other."""
    conflicts = []

    for test_input in test_cases:
        matches = []
        for i, (pattern, token_type) in enumerate(patterns):
            if re.match(pattern, test_input):
                matches.append((i, token_type, pattern))

        if len(matches) > 1:
            conflicts.append((test_input, matches))

    return conflicts

# Usage
test_cases = ["IF", "<=", "[field]", "123.45", '"string"']
patterns = TOKEN_PATTERNS
conflicts = find_pattern_conflicts(patterns, test_cases)

for test_input, matches in conflicts:
    print(f"'{test_input}' matches multiple patterns:")
    for index, token_type, pattern in matches:
        print(f"  {index}: {token_type} ({pattern})")
```

### Token Stream Validator
```python
def validate_token_stream(formula, expected_types):
    """Validate that tokenization produces expected token types."""
    lexer = FormulaLexer()
    tokens = lexer.tokenize(formula)

    # Remove EOF token for comparison
    actual_types = [token.type for token in tokens[:-1]]

    if actual_types == expected_types:
        print("✓ Tokenization matches expected types")
        return True
    else:
        print("✗ Tokenization mismatch")
        print(f"Expected: {[t.value for t in expected_types]}")
        print(f"Actual:   {[t.value for t in actual_types]}")
        return False

# Usage
from tableau_to_looker_parser.models.parser_models import TokenType
validate_token_stream(
    "IF [adult] THEN 'Yes' END",
    [TokenType.IF, TokenType.FIELD_REF, TokenType.THEN, TokenType.STRING, TokenType.END]
)
```

### Pattern Performance Profiler
```python
import time

def profile_pattern_performance(patterns, test_cases, iterations=1000):
    """Profile how long each pattern takes to match."""
    results = {}

    for i, (pattern, token_type) in enumerate(patterns):
        compiled_pattern = re.compile(pattern)
        total_time = 0

        for _ in range(iterations):
            for test_case in test_cases:
                start_time = time.perf_counter()
                compiled_pattern.match(test_case)
                total_time += time.perf_counter() - start_time

        results[f"{i}_{token_type}"] = total_time / (iterations * len(test_cases))

    # Sort by time
    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)

    print("Pattern performance (avg time per match):")
    for pattern_name, avg_time in sorted_results:
        print(f"  {pattern_name}: {avg_time:.6f}s")

    return results
```

## Debugging Checklist

When tokenization fails:

### ✅ Step 1: Verify Input
- [ ] Check for special characters or encoding issues
- [ ] Verify string is not empty or None
- [ ] Look for unexpected whitespace

### ✅ Step 2: Print Token Stream
- [ ] Run `debug_tokenization()` on the failing formula
- [ ] Check for `UNKNOWN` tokens
- [ ] Verify token positions make sense

### ✅ Step 3: Check Pattern Order
- [ ] Are container patterns (strings, field refs) first?
- [ ] Are keywords before generic identifiers?
- [ ] Are multi-character operators before single-character?

### ✅ Step 4: Test Individual Patterns
- [ ] Test failing pattern in isolation
- [ ] Check for regex syntax errors
- [ ] Verify pattern captures correct groups

### ✅ Step 5: Look for Conflicts
- [ ] Run pattern conflict detector
- [ ] Check if multiple patterns match same input
- [ ] Verify intended pattern wins

### ✅ Step 6: Validate Results
- [ ] Compare actual vs expected token types
- [ ] Check token values are extracted correctly
- [ ] Verify special processing (quote removal, etc.) works

## Common Regex Issues

### Issue 1: Forgetting to Escape Special Characters
```python
# ❌ Wrong - [ ] are regex special characters
(r"[([^\]]+)]", TokenType.FIELD_REF)

# ✅ Correct - escape the brackets
(r"\[([^\]]+)\]", TokenType.FIELD_REF)
```

### Issue 2: Missing Word Boundaries
```python
# ❌ Wrong - matches partial words
(r"(?i)IF", TokenType.IF)  # Matches "DIFF", "IFFY"

# ✅ Correct - only complete words
(r"(?i)\bIF\b", TokenType.IF)  # Only "IF"
```

### Issue 3: Incorrect Quantifiers
```python
# ❌ Wrong - * allows empty match
(r"\[([^\]]*)\]", TokenType.FIELD_REF)  # Matches "[]"

# ✅ Correct - + requires at least one character
(r"\[([^\]]+)\]", TokenType.FIELD_REF)  # Requires content
```

### Issue 4: Case Sensitivity Problems
```python
# ❌ Wrong - case sensitive
(r"\bIF\b", TokenType.IF)  # Only matches "IF"

# ✅ Correct - case insensitive
(r"(?i)\bIF\b", TokenType.IF)  # Matches "IF", "if", "If"
```

## Testing Strategies

### Unit Tests for Tokenization
```python
import pytest
from tableau_to_looker_parser.converters.formula_parser import FormulaLexer
from tableau_to_looker_parser.models.parser_models import TokenType

class TestTokenization:
    def setup_method(self):
        self.lexer = FormulaLexer()

    def test_simple_field_reference(self):
        tokens = self.lexer.tokenize("[adult]")
        assert len(tokens) == 2  # FIELD_REF + EOF
        assert tokens[0].type == TokenType.FIELD_REF
        assert tokens[0].value == "adult"

    def test_keyword_recognition(self):
        tokens = self.lexer.tokenize("IF")
        assert tokens[0].type == TokenType.IF
        assert tokens[0].value == "IF"

    def test_operators_not_split(self):
        tokens = self.lexer.tokenize("<=")
        assert len(tokens) == 2  # LESS_EQUAL + EOF
        assert tokens[0].type == TokenType.LESS_EQUAL
        assert tokens[0].value == "<="

    @pytest.mark.parametrize("formula,expected_types", [
        ("IF [adult] THEN 'Yes' END",
         [TokenType.IF, TokenType.FIELD_REF, TokenType.THEN, TokenType.STRING, TokenType.END]),
        ("[budget] + [revenue]",
         [TokenType.FIELD_REF, TokenType.PLUS, TokenType.FIELD_REF]),
    ])
    def test_complex_formulas(self, formula, expected_types):
        tokens = self.lexer.tokenize(formula)
        actual_types = [t.type for t in tokens[:-1]]  # Exclude EOF
        assert actual_types == expected_types
```

This troubleshooting guide should help you quickly identify and fix tokenization issues!
