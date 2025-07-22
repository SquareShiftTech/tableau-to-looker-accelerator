import json
from tableau_to_looker_parser.converters.formula_parser import FormulaParser
from tableau_to_looker_parser.handlers.calculated_field_handler import (
    CalculatedFieldHandler,
)

parser = FormulaParser()
handler = CalculatedFieldHandler()

# Test formulas that use different AST node fields
examples = [
    {
        "name": "Unary_Example",
        "formula": "-[budget]",
        "type": "Unary Operation (uses operand)",
    },
    {
        "name": "Comparison_Example",
        "formula": "[budget] > 1000",
        "type": "Comparison (uses left, right, operator)",
    },
    {
        "name": "Logical_Example",
        "formula": "[adult] AND [budget] > 1000",
        "type": "Logical Operation (uses left, right, operator)",
    },
    {
        "name": "Nested_If_Example",
        "formula": 'IF [budget] > 1000 THEN "High" ELSE "Low" END',
        "type": "Conditional with comparison (condition, then_branch, else_branch)",
    },
]

results = {}

for example in examples:
    print(f"\n=== {example['type']} ===")
    print(f"Formula: {example['formula']}")

    result = parser.parse_formula(example["formula"], example["name"], "dimension")

    if result.success:
        field_data = {
            "name": f"[{example['name']}]",
            "role": "dimension",
            "datatype": "string",
            "calculation": example["formula"],
        }

        json_result = handler.convert_to_json(field_data)
        ast = json_result["calculation"]["ast"]

        print(f"Root AST Node Type: {ast['node_type']}")

        # Show which fields are actually used (non-null)
        used_fields = []
        for field in [
            "operator",
            "left",
            "right",
            "operand",
            "condition",
            "then_branch",
            "else_branch",
            "function_name",
            "arguments",
        ]:
            value = ast.get(field)
            if value is not None and value != []:
                if field in [
                    "left",
                    "right",
                    "operand",
                    "condition",
                    "then_branch",
                    "else_branch",
                ]:
                    used_fields.append(f"{field}: {value['node_type']}")
                elif field == "arguments":
                    used_fields.append(f"{field}: [{len(value)} items]")
                else:
                    used_fields.append(f"{field}: {value}")

        print(f"Used fields: {used_fields}")

        results[example["name"]] = {
            "formula": example["formula"],
            "type": example["type"],
            "json": json_result,
        }

    else:
        print(f"ERROR: {result.error_message}")

# Save results
with open("sample_twb_files/generated_json_book5/ast_fields_examples.json", "w") as f:
    json.dump(results, f, indent=2, default=str)

print("\n=== AST Field Documentation ===")
print(
    "operator     -> Used for arithmetic (+,-,*,/), comparison (=,>,<), logical (AND,OR)"
)
print("left/right   -> Used for binary operations (arithmetic, comparison, logical)")
print("operand      -> Used for unary operations (-[field], NOT [condition])")
print("condition    -> Used for IF statements (the condition to evaluate)")
print("then_branch  -> Used for IF statements (what to return if condition is true)")
print("else_branch  -> Used for IF statements (what to return if condition is false)")
print("case_expression -> Used for CASE statements (the expression being evaluated)")
print("when_clauses -> Used for CASE statements (array of WHEN conditions)")
print("function_name -> Used for function calls (SUM, COUNT, ROUND, etc.)")
print("arguments    -> Used for function calls (array of function parameters)")
print("field_name   -> Used for field references ([Field Name])")
print("value        -> Used for literals (strings, numbers, booleans)")
print("items        -> Used for lists/arrays (IN clauses, etc.)")
print("min_value/max_value -> Used for BETWEEN clauses")

print(
    "\nResults saved to: sample_twb_files/generated_json_book5/ast_fields_examples.json"
)
