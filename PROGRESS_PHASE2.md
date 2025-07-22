# Tableau to LookML Migration - Phase 2 Progress Tracking

## Overview
This document tracks the completion status of Phase 2 requirements focusing on advanced handlers and business logic, with **calculated fields as the top priority**.

## Phase 2 Focus: Advanced Handlers & Business Logic

### Phase 2.1: Calculated Field Handler (TOP PRIORITY) 🔥

#### AST-Based Approach for Calculated Fields
**Strategy:** Tableau Formula → AST (JSON) → Comprehensive Testing → LookML Generated Field

**Updated Implementation Plan:**
1. **AST Schema**: Unified, scalable JSON representation for all formula types
2. **Formula Parser**: Convert Tableau formulas to AST with comprehensive testing
3. **JSON Integration**: Integrate AST into existing JSON schema
4. **Testing Phase**: Extensive testing with multiple formula types before LookML
5. **LookML Generator**: Render calculated fields from tested AST (Future Phase)

#### Task 2.1.1: Unified AST Schema Design ⏳ PENDING
**Status:** Ready to implement
```
Create: src/tableau_to_looker_parser/models/ast_schema.py

Requirements (Unified Approach):
- Single ASTNode class handling all node types
- Scalable design supporting future extensions
- Based on demo JSON structure from ast_parser_demo_results.json
- Fields: node_type, operator, left/right, condition/then_branch/else_branch
- Function calls, field references, literals in one unified structure
- Extensible properties dict for future node types
```

#### Task 2.1.2: Formula Parser Implementation ⏳ PENDING
**Status:** Not Started
```
Create: src/tableau_to_looker_parser/converters/formula_parser.py

Requirements:
- FormulaParser class using unified AST nodes
- Support for demo patterns: arithmetic, conditional, function, field, literal
- Tokenization and parsing for Tableau syntax
- Error handling and validation
- Confidence scoring and complexity analysis
```

#### Task 2.1.3: Calculated Field Handler ⏳ PENDING
**Status:** Not Started
```
Create: src/tableau_to_looker_parser/handlers/calculated_field_handler.py

Requirements:
- CalculatedFieldHandler extending BaseHandler
- Integration with FormulaParser
- AST generation and validation
- Field dependency tracking
- Integration with existing handler system
```

#### Task 2.1.4: JSON Schema Extension ⏳ PENDING
**Status:** Not Started
```
Update: src/tableau_to_looker_parser/models/json_schema.py

Requirements:
- Extend DimensionSchema and MeasureSchema with AST support
- Add calculated_field_ast: Optional[CalculatedFieldAST] field
- Maintain backward compatibility
- Support both regular and calculated fields
```

#### Task 2.1.5: Comprehensive Test Suite 🔥 HIGH PRIORITY
**Status:** Not Started
```
Create extensive test files:
- tests/test_ast_schema.py - Test AST node creation and validation
- tests/test_formula_parser.py - Test all formula parsing scenarios
- tests/test_calculated_field_handler.py - Test handler integration
- tests/integration/test_calculated_fields_ast.py - End-to-end AST tests

Test Cases:
- Simple arithmetic: [Sales] + [Profit]
- Conditionals: IF [Sales] > 1000 THEN "High" ELSE "Low" END
- Functions: SUM([Revenue]), UPPER([Customer Name])
- Complex nested: IF SUM([Sales]) > 10000 THEN "Target Met" ELSE "Below Target" END
- Error cases: Invalid syntax, unsupported functions
```

#### Task 2.1.6: LookML Generator Extension (FUTURE PHASE) 📋 LOW PRIORITY
**Status:** Deferred until after testing
```
Update: LookML generation system (AFTER comprehensive testing)

Requirements:
- AST to SQL renderer
- Template system updates
- Generated LookML validation
```

### Phase 2.2: Enhanced Parsing & Testing

#### Task 2.2.1: XML Parser Enhancement ⏳ PENDING
**Status:** Not Started
```
Update: src/tableau_to_looker_parser/core/xml_parser.py

Requirements:
- Enhanced calculated field extraction from <calculation> elements
- Support for complex nested formulas
- Field dependency resolution from formula text
- Integration with existing dimension/measure extraction
```

#### Task 2.2.2: Migration Engine Integration ⏳ PENDING
**Status:** Not Started
```
Update: src/tableau_to_looker_parser/core/migration_engine.py

Requirements:
- Register CalculatedFieldHandler with appropriate priority
- Update element processing to handle calculated fields
- Ensure proper handler orchestration
```

#### Task 2.2.3: Comprehensive Testing ⏳ PENDING
**Status:** Not Started
```
Create test files:
- tests/test_formula_parser.py
- tests/test_calculated_field_handler.py
- tests/test_ast_schema.py
- tests/integration/test_calculated_fields_integration.py

Requirements:
- Unit tests for all formula parsing scenarios
- Integration tests with sample workbooks containing calculated fields
- Performance tests for complex formulas
- Error handling tests for malformed formulas
```

## Priority Functions to Support (Phase 2.1)

### High Priority Functions (Must Have)
- **Conditional Logic**: IF, IIF, CASE, WHEN
- **Mathematical**: +, -, *, /, %, ABS, ROUND, CEIL, FLOOR
- **String Functions**: LEFT, RIGHT, MID, LEN, CONTAINS, UPPER, LOWER
- **Date Functions**: DATEADD, DATEDIFF, YEAR, MONTH, DAY
- **Aggregation**: SUM, COUNT, AVG, MIN, MAX (for measures)
- **Logical**: AND, OR, NOT, ISNULL, IFNULL

### Medium Priority Functions (Should Have)
- **Advanced Math**: POWER, SQRT, LOG, EXP
- **String Advanced**: TRIM, LTRIM, RTRIM, REPLACE, SPLIT
- **Date Advanced**: DATEPART, DATENAME, NOW, TODAY
- **Comparison**: BETWEEN, IN

### Low Priority Functions (Nice to Have)
- **Statistical**: STDEV, VAR, MEDIAN, PERCENTILE
- **Window Functions**: RUNNING_SUM, WINDOW_SUM, RANK
- **LOD Expressions**: FIXED, INCLUDE, EXCLUDE (basic support)

## Sample Test Cases

### Simple Calculated Field
```
Tableau: IF [Sales] > 1000 THEN "High" ELSE "Low" END
AST: {
  "type": "if_statement",
  "condition": {
    "type": "comparison",
    "operator": ">",
    "left": {"type": "field_ref", "field": "Sales"},
    "right": {"type": "literal", "value": 1000, "data_type": "integer"}
  },
  "then_value": {"type": "literal", "value": "High", "data_type": "string"},
  "else_value": {"type": "literal", "value": "Low", "data_type": "string"}
}
LookML:
dimension: sales_category {
  type: string
  case: {
    when: {
      sql: ${sales} > 1000 ;;
      label: "High"
    }
    else: "Low"
  }
}
```

### Complex Calculated Field
```
Tableau: CASE [Region] WHEN "North" THEN [Sales] * 1.1 WHEN "South" THEN [Sales] * 1.05 ELSE [Sales] END
AST: {
  "type": "case_statement",
  "cases": [
    {
      "when": {"type": "literal", "value": "North", "data_type": "string"},
      "then": {
        "type": "arithmetic",
        "operator": "*",
        "left": {"type": "field_ref", "field": "Sales"},
        "right": {"type": "literal", "value": 1.1, "data_type": "real"}
      }
    },
    {
      "when": {"type": "literal", "value": "South", "data_type": "string"},
      "then": {
        "type": "arithmetic",
        "operator": "*",
        "left": {"type": "field_ref", "field": "Sales"},
        "right": {"type": "literal", "value": 1.05, "data_type": "real"}
      }
    }
  ],
  "else": {"type": "field_ref", "field": "Sales"}
}
```

## Success Criteria for Phase 2.1

- ✅ Parse 80%+ of common calculated field formulas
- ✅ Generate valid LookML for dimension and measure calculated fields
- ✅ Handle nested expressions and complex logic
- ✅ Proper field dependency tracking
- ✅ Comprehensive test coverage (80%+)
- ✅ Integration with existing Phase 1 components
- ✅ Performance acceptable for workbooks with 50+ calculated fields

## Current Status Summary

### COMPLETED ✅
- Phase 1 foundation fully implemented and tested
- Core architecture ready for Phase 2 extensions

### IN PROGRESS ⏳
- Phase 2.1 Calculated Field Handler (Priority #1)
- AST-based formula parsing approach

### PENDING ❌
- All Phase 2.1 tasks (7 tasks total)
- Integration testing with calculated fields
- Performance optimization for complex formulas

## Updated Next Steps (Focus on AST → JSON → Testing)

### Phase 2A: AST and JSON Generation (CURRENT FOCUS)
1. **IMMEDIATE**: Implement unified AST schema (`ast_schema.py`)
2. **NEXT**: Create formula parser with comprehensive tokenization (`formula_parser.py`)
3. **THEN**: Build calculated field handler with AST integration
4. **THEN**: Update JSON schema to include AST data
5. **PRIORITY**: Create comprehensive test suite with multiple formula types

### Phase 2B: LookML Generation (FUTURE)
6. **LATER**: Extend LookML generator for calculated fields (after thorough testing)
7. **FINALLY**: Template system updates and validation

### Testing Strategy
- **Unit Tests**: Each component (AST, Parser, Handler) separately
- **Integration Tests**: Full Tableau formula → AST → JSON pipeline
- **Validation Tests**: AST structure validation and field dependency tracking
- **Error Handling Tests**: Malformed formulas and unsupported functions

## Dependencies

- Phase 1 components (completed) ✅
- Jinja2 template system (available) ✅
- Pydantic schema validation (available) ✅
- XML parsing infrastructure (available) ✅

---
*Last Updated: 2025-01-22*
*Status: Phase 2A - AST Schema & Parser Development (Testing-Focused)*
*Next Milestone: Complete AST → JSON pipeline with comprehensive testing*
