"""
SQLParse-based View LookML generator (experimental).
Alternative implementation using sqlparse tokens instead of custom AST.
"""

from typing import Dict, List, Set
import logging
import re
from types import SimpleNamespace

from .base_generator import BaseGenerator

try:
    import sqlparse
    from sqlparse import tokens as T

    SQLPARSE_AVAILABLE = True
except ImportError:
    SQLPARSE_AVAILABLE = False
    print("Warning: sqlparse not available, falling back to string replacement")

logger = logging.getLogger(__name__)


class ViewGeneratorSQLParse(BaseGenerator):
    """Generator for view.lkml files using sqlparse tokens."""

    def __init__(self, template_engine=None):
        """Initialize the sqlparse-based view generator."""
        super().__init__(template_engine)

        # Tableau to SQL function mappings
        self.function_mappings = {
            "LEN": "LENGTH",
            "MID": "SUBSTR",
            "UPPER": "UPPER",
            "LOWER": "LOWER",
            "TRIM": "TRIM",
            "LEFT": "LEFT",
            "RIGHT": "RIGHT",
            "CONTAINS": self._convert_contains,
            "STARTSWITH": self._convert_startswith,
            "ENDSWITH": self._convert_endswith,
            "FIND": self._convert_find,
            "YEAR": "EXTRACT(YEAR FROM {})",
            "MONTH": "EXTRACT(MONTH FROM {})",
            "DAY": "EXTRACT(DAY FROM {})",
            "NOW": "CURRENT_TIMESTAMP",
            "TODAY": "CURRENT_DATE",
        }

    def generate_views(self, migration_data: Dict, output_dir: str) -> List[str]:
        """
        Generate all view.lkml files for a migration using sqlparse.
        """
        if not SQLPARSE_AVAILABLE:
            logger.warning("SQLParse not available, using fallback method")
            return self._generate_views_fallback(migration_data, output_dir)

        try:
            view_files = []
            all_dimensions = migration_data.get("dimensions", [])
            all_measures = migration_data.get("measures", [])
            all_calculated_fields = migration_data.get("calculated_fields", [])

            # Determine which views need to be generated
            view_names_needed = self._determine_view_names(migration_data)

            # Generate view file for each needed view
            for view_name in view_names_needed:
                view_file = self._generate_single_view(
                    view_name,
                    migration_data,
                    all_dimensions,
                    all_measures,
                    all_calculated_fields,
                    output_dir,
                )
                if view_file:
                    view_files.append(view_file)

            logger.info(f"Generated {len(view_files)} view files using sqlparse")
            return view_files

        except Exception as e:
            logger.error(f"Failed to generate view files with sqlparse: {str(e)}")
            raise

    def _determine_view_names(self, migration_data: Dict) -> Set[str]:
        """Determine which view names are needed (tables + self-join aliases)."""
        view_names_needed = set()

        # Add actual table names
        for table in migration_data.get("tables", []):
            view_names_needed.add(table["name"])

        # Add self-join aliases
        self_join_aliases = self._detect_self_join_aliases(migration_data)
        view_names_needed.update(self_join_aliases)

        return view_names_needed

    def _detect_self_join_aliases(self, migration_data: Dict) -> Set[str]:
        """Detect self-join aliases that need separate view files."""
        self_join_aliases = set()
        actual_table_names = [
            table["name"] for table in migration_data.get("tables", [])
        ]

        for relationship in migration_data.get("relationships", []):
            if relationship.get("relationship_type") == "physical":
                table_aliases = relationship.get("table_aliases", {})

                # Group aliases by their table reference
                table_refs = {}
                for alias, table_ref in table_aliases.items():
                    if table_ref not in table_refs:
                        table_refs[table_ref] = []
                    table_refs[table_ref].append(alias)

                # For each table that has multiple aliases, it's a self-join
                for table_ref, aliases in table_refs.items():
                    if len(aliases) > 1:  # Multiple aliases for same table = self-join
                        for alias in aliases:
                            if (
                                alias not in actual_table_names
                            ):  # Only add non-table aliases
                                self_join_aliases.add(alias)

        return self_join_aliases

    def _generate_single_view(
        self,
        view_name: str,
        migration_data: Dict,
        all_dimensions: List[Dict],
        all_measures: List[Dict],
        all_calculated_fields: List[Dict],
        output_dir: str,
    ) -> str:
        """Generate a single view file using sqlparse."""
        # Find the actual table this view represents
        actual_table, table_ref = self._resolve_view_table(view_name, migration_data)

        if not actual_table:
            logger.warning(f"Could not resolve table for view: {view_name}")
            return None

        actual_table_name = actual_table["name"]

        # Filter dimensions and measures for this specific table
        table_dimensions = [
            dim for dim in all_dimensions if dim.get("table_name") == actual_table_name
        ]
        table_measures = [
            measure
            for measure in all_measures
            if measure.get("table_name") == actual_table_name
        ]

        # Filter calculated fields for this specific table and convert using sqlparse
        table_calculated_fields = []
        table_calculated_dimensions = []
        for calc_field in all_calculated_fields:
            if calc_field.get("table_name") == actual_table_name:
                # Convert calculated field using sqlparse
                converted_field = self._convert_calculated_field_sqlparse(
                    calc_field, view_name
                )
                if converted_field:
                    # Check if this is a two-step pattern
                    if converted_field.get("two_step_pattern"):
                        table_calculated_dimensions.append(converted_field["dimension"])
                        table_calculated_fields.append(converted_field["measure"])
                    else:
                        table_calculated_fields.append(converted_field)

        # Build view data
        view_data = {
            "name": view_name,
            "table_name": table_ref,
            "dimensions": table_dimensions,
            "measures": table_measures,
            "calculated_fields": table_calculated_fields,
            "calculated_dimensions": table_calculated_dimensions,
        }

        # Generate the view file
        return self._create_view_file(view_data, output_dir)

    def _resolve_view_table(self, view_name: str, migration_data: Dict) -> tuple:
        """Resolve view name to actual table and table reference."""
        # First check if it's an actual table name
        for table in migration_data.get("tables", []):
            if table["name"] == view_name:
                return table, table["table"]

        # If not found, check if it's an alias
        for relationship in migration_data.get("relationships", []):
            table_aliases = relationship.get("table_aliases", {})
            if view_name in table_aliases:
                table_ref = table_aliases[view_name]
                # Find the actual table with this reference
                for table in migration_data.get("tables", []):
                    if table["table"] == table_ref:
                        return table, table_ref

        return None, None

    def _convert_calculated_field_sqlparse(
        self, calc_field: Dict, table_context: str
    ) -> Dict:
        """
        Convert a calculated field using sqlparse token processing.
        """
        try:
            # Extract formula from calculation
            calculation = calc_field.get("calculation", {})
            original_formula = calculation.get("original_formula", "")

            if not original_formula:
                logger.warning(
                    f"No formula found for calculated field: {calc_field.get('name')}"
                )
                return None

            logger.info(
                f"Converting calculated field using sqlparse: {calc_field.get('name')}"
            )
            logger.info(f"Original formula: {original_formula}")

            # Convert Tableau formula to LookML SQL using sqlparse
            lookml_sql = self._tableau_to_lookml_sqlparse(original_formula)
            logger.info(f"Converted SQL: {lookml_sql}")

            # Check if conversion failed
            is_fallback = "MIGRATION_REQUIRED" in lookml_sql or lookml_sql.strip() == ""

            # Check if this is a measure that needs two-step pattern
            role = calc_field.get("role", "dimension")
            if role == "measure" and self._needs_two_step_pattern(
                calc_field, calculation
            ):
                return self._create_two_step_pattern(
                    calc_field, lookml_sql, calculation
                )

            # Standard single field conversion
            lookml_type = self._determine_lookml_type(calc_field, calculation)

            # Build LookML field definition
            converted_field = {
                "name": self._clean_name(calc_field.get("name", "")),
                "original_name": calc_field.get("original_name", ""),
                "field_type": calc_field.get("field_type", "dimension"),
                "role": calc_field.get("role", "dimension"),
                "datatype": calc_field.get("datatype", "string"),
                "sql": lookml_sql,
                "original_formula": original_formula,
                "description": f"Calculated field: {self._normalize_formula_for_description(original_formula)}",
                "lookml_type": lookml_type,
            }

            # Add migration metadata for fallback fields
            if is_fallback:
                converted_field.update(
                    {
                        "migration_error": True,
                        "migration_comment": f"MIGRATION ERROR - Manual conversion required\nOriginal formula: {original_formula}",
                    }
                )

            return converted_field

        except Exception as e:
            logger.error(
                f"Failed to convert calculated field {calc_field.get('name')} with sqlparse: {str(e)}"
            )
            return self._create_fallback_lookml_field(calc_field, str(e))

    def _tableau_to_lookml_sqlparse(self, tableau_formula: str) -> str:
        """
        Convert Tableau formula to LookML SQL using sqlparse tokens.
        """
        if not SQLPARSE_AVAILABLE:
            return self._tableau_to_lookml_fallback(tableau_formula)

        try:
            logger.debug(f"Processing formula with sqlparse: {tableau_formula}")

            # Step 1: Preprocess field references for sqlparse
            preprocessed = self._preprocess_field_references(tableau_formula)
            logger.debug(f"After field preprocessing: {preprocessed}")

            # Step 2: Handle IF statements conversion
            preprocessed = self._convert_if_statements_sqlparse(preprocessed)
            logger.debug(f"After IF conversion: {preprocessed}")

            # Step 3: Parse with sqlparse
            parsed = sqlparse.parse(preprocessed)[0]

            # Step 4: Process tokens and convert
            converted_tokens = []
            self._process_sqlparse_tokens(parsed.tokens, converted_tokens)

            # Step 5: Join tokens back into SQL
            result = "".join(converted_tokens)

            # Step 6: Post-process - restore field references
            result = self._postprocess_field_references(result)

            return result.strip()

        except Exception as e:
            logger.error(f"SQLParse conversion failed: {str(e)}")
            return self._tableau_to_lookml_fallback(tableau_formula)

    def _preprocess_field_references(self, formula: str) -> str:
        """Replace [Field Name] with __FIELD_fieldname__ tokens for sqlparse."""

        def replace_field(match):
            field_name = match.group(1)
            # Convert to token-safe format
            safe_name = field_name.lower().replace(" ", "_").replace("-", "_")
            return f"__FIELD_{safe_name}__"

        return re.sub(r"\[([^\]]+)\]", replace_field, formula)

    def _postprocess_field_references(self, sql: str) -> str:
        """Convert __FIELD_fieldname__ back to ${TABLE}.fieldname."""

        def restore_field(match):
            field_name = match.group(1)
            return f"${{TABLE}}.{field_name}"

        return re.sub(r"__FIELD_([^_]+)__", restore_field, sql)

    def _convert_if_statements_sqlparse(self, formula: str) -> str:
        """Convert IF-THEN-ELSE to CASE-WHEN-ELSE using sqlparse tokens."""
        if "IF " not in formula.upper():
            return formula

        try:
            # Simple IF-THEN-ELSE pattern replacement
            # This handles basic cases - more complex nesting would need token-level processing
            pattern = r"\bIF\s+(.*?)\s+THEN\s+(.*?)\s+ELSE\s+(.*?)\s+END\b"

            def replace_if(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                return f"CASE WHEN {condition} THEN {then_value} ELSE {else_value} END"

            result = re.sub(pattern, replace_if, formula, flags=re.IGNORECASE)
            logger.debug(f"IF conversion: {formula} -> {result}")
            return result

        except Exception as e:
            logger.warning(f"IF statement conversion failed: {e}")
            return formula

    def _process_sqlparse_tokens(self, tokens, output):
        """Process sqlparse tokens and convert them."""
        for token in tokens:
            if hasattr(token, "tokens") and token.tokens:
                # Nested token list - recurse
                self._process_sqlparse_tokens(token.tokens, output)
            else:
                # Single token - process it
                converted = self._process_single_sqlparse_token(token)
                output.append(converted)

    def _process_single_sqlparse_token(self, token):
        """Process a single sqlparse token and convert if needed."""
        token_value = str(token)
        token_type = token.ttype

        logger.debug(f"Processing token: '{token_value}' (Type: {token_type})")

        # Handle different token types
        if token_type is T.Name:
            # Check if it's a Tableau function that needs conversion
            upper_value = token_value.upper()
            if upper_value in self.function_mappings:
                mapping = self.function_mappings[upper_value]
                if callable(mapping):
                    # Special function conversion (handled separately)
                    return token_value  # Return as-is, special handling done elsewhere
                else:
                    logger.debug(f"Converting function: {token_value} -> {mapping}")
                    return mapping
            return token_value

        elif token_type is T.Operator:
            # Handle operator conversions
            if token_value == "<>":
                return "!="
            elif token_value == "^":
                return "POWER"  # This would need special handling for syntax
            return token_value

        else:
            # For all other tokens (strings, numbers, keywords, etc.), return as-is
            return token_value

    # Function conversion helpers
    def _convert_contains(self, args):
        """Convert CONTAINS(string, substring) to POSITION(substring IN string) > 0"""
        if len(args) != 2:
            return f"CONTAINS({', '.join(args)})"  # Fallback
        return f"POSITION({args[1]} IN {args[0]}) > 0"

    def _convert_startswith(self, args):
        """Convert STARTSWITH(string, prefix) to LEFT(string, LENGTH(prefix)) = prefix"""
        if len(args) != 2:
            return f"STARTSWITH({', '.join(args)})"  # Fallback
        return f"LEFT({args[0]}, LENGTH({args[1]})) = {args[1]}"

    def _convert_endswith(self, args):
        """Convert ENDSWITH(string, suffix) to RIGHT(string, LENGTH(suffix)) = suffix"""
        if len(args) != 2:
            return f"ENDSWITH({', '.join(args)})"  # Fallback
        return f"RIGHT({args[0]}, LENGTH({args[1]})) = {args[1]}"

    def _convert_find(self, args):
        """Convert FIND(string, substring) to POSITION(substring IN string)"""
        if len(args) != 2:
            return f"FIND({', '.join(args)})"  # Fallback
        return f"POSITION({args[1]} IN {args[0]})"

    # Fallback methods when sqlparse is not available
    def _tableau_to_lookml_fallback(self, tableau_formula: str) -> str:
        """Fallback method using string replacement when sqlparse is not available."""
        logger.warning("Using fallback string replacement method")

        # Simple string replacements
        result = tableau_formula

        # Replace field references
        result = re.sub(
            r"\[([^\]]+)\]",
            lambda m: f"${{TABLE}}.{m.group(1).lower().replace(' ', '_')}",
            result,
        )

        # Replace basic functions
        for tableau_func, sql_func in [("LEN", "LENGTH"), ("MID", "SUBSTR")]:
            result = result.replace(f"{tableau_func}(", f"{sql_func}(")

        # Simple IF-THEN-ELSE replacement
        result = re.sub(
            r"\bIF\s+(.*?)\s+THEN\s+(.*?)\s+ELSE\s+(.*?)\s+END\b",
            r"CASE WHEN \1 THEN \2 ELSE \3 END",
            result,
            flags=re.IGNORECASE,
        )

        return result

    def _generate_views_fallback(
        self, migration_data: Dict, output_dir: str
    ) -> List[str]:
        """Fallback view generation when sqlparse is not available."""
        logger.warning("Falling back to original view generation method")
        # This would call the original view generator
        return []

    # Utility methods (copied from original view_generator.py)
    def _needs_two_step_pattern(self, calc_field: Dict, calculation: Dict) -> bool:
        """Determine if a calculated field measure needs the two-step pattern."""
        if calc_field.get("role") != "measure":
            return False

        original_formula = calculation.get("original_formula", "")
        dependencies = calculation.get("dependencies", [])

        if not dependencies:
            return False

        # If formula already contains aggregation, no need for two-step pattern
        agg_functions = ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "MEDIAN("]
        has_aggregation = any(
            func in original_formula.upper() for func in agg_functions
        )

        return not has_aggregation

    def _determine_lookml_type(self, calc_field: Dict, calculation: Dict) -> str:
        """Determine the appropriate LookML type for a calculated field."""
        field_role = calc_field.get("role", "dimension")

        if field_role == "dimension":
            datatype = calc_field.get("datatype", "string")
            if datatype == "boolean":
                return "yesno"
            elif datatype in ["integer", "real"]:
                return "number"
            else:
                return "string"
        else:
            # For measures
            original_formula = calculation.get("original_formula", "")
            agg_functions = ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "MEDIAN("]
            has_aggregation = any(
                func in original_formula.upper() for func in agg_functions
            )

            return "number" if has_aggregation else "sum"

    def _create_two_step_pattern(
        self, calc_field: Dict, lookml_sql: str, calculation: Dict
    ) -> Dict:
        """Create two-step pattern: hidden dimension + aggregated measure."""
        base_name = self._clean_name(calc_field.get("name", ""))
        original_formula = calculation.get("original_formula", "")

        dimension_field = {
            "name": f"{base_name}_calc",
            "original_name": calc_field.get("original_name", ""),
            "field_type": "dimension",
            "role": "dimension",
            "datatype": calc_field.get("datatype", "real"),
            "sql": lookml_sql,
            "original_formula": original_formula,
            "description": f"Row-level calculation for {base_name}: {self._normalize_formula_for_description(original_formula)}",
            "lookml_type": "number",
            "hidden": True,
        }

        measure_field = {
            "name": base_name,
            "original_name": calc_field.get("original_name", ""),
            "field_type": "measure",
            "role": "measure",
            "datatype": calc_field.get("datatype", "real"),
            "sql": f"${{{base_name}_calc}}",
            "original_formula": original_formula,
            "description": f"Calculated field: {self._normalize_formula_for_description(original_formula)}",
            "lookml_type": "sum",
        }

        return {
            "two_step_pattern": True,
            "dimension": dimension_field,
            "measure": measure_field,
        }

    def _create_fallback_lookml_field(
        self, calc_field: Dict, error_message: str
    ) -> Dict:
        """Create a fallback LookML field when conversion fails."""
        calculation = calc_field.get("calculation", {})
        original_formula = calculation.get("original_formula", "UNKNOWN_FORMULA")
        field_name = calc_field.get("name", "unknown_field")

        return {
            "name": self._clean_name(field_name),
            "original_name": calc_field.get("original_name", ""),
            "field_type": calc_field.get("field_type", "dimension"),
            "role": calc_field.get("role", "dimension"),
            "sql": "'MIGRATION_REQUIRED'",
            "original_formula": original_formula,
            "description": "MIGRATION ERROR - Manual conversion required",
            "lookml_type": "string",
            "migration_error": True,
            "migration_comment": f"CONVERSION ERROR: {error_message}\nOriginal formula: {original_formula}",
        }

    def _normalize_formula_for_description(self, formula: str) -> str:
        """Normalize multi-line formulas for use in descriptions."""
        if not formula:
            return formula
        single_line = re.sub(r"\s*\n\s*", " ", formula.strip())
        return re.sub(r"\s+", " ", single_line)

    def _format_table_name(self, table_name: str) -> str:
        """Format table name from [schema].[table] to `schema.table`."""
        if not table_name:
            return table_name
        formatted = table_name.replace("[", "").replace("]", "").replace(".", ".")
        return f"`{formatted}`"

    def _create_view_file(self, view_data: Dict, output_dir: str) -> str:
        """Create a single view file from view data."""
        try:
            # Prepare template context (same as original)
            context = {
                "view": SimpleNamespace(**view_data),
                "view_name": self._clean_name(view_data["name"]),
                "table_name": self._format_table_name(view_data["table_name"]),
                "dimensions": view_data["dimensions"],
                "measures": view_data["measures"],
                "calculated_fields": view_data["calculated_fields"],
                "calculated_dimensions": view_data.get("calculated_dimensions", []),
                "has_dimensions": len(view_data["dimensions"]) > 0,
                "has_measures": len(view_data["measures"]) > 0,
                "has_calculated_fields": len(view_data["calculated_fields"]) > 0,
                "has_calculated_dimensions": len(
                    view_data.get("calculated_dimensions", [])
                )
                > 0,
            }

            # Render template
            content = self.template_engine.render_template("basic_view.j2", context)

            # Write to file
            output_path = self._ensure_output_dir(output_dir)
            view_filename = f"{self._clean_name(view_data['name'])}{self.view_extension}{self.lookml_extension}"
            file_path = output_path / view_filename

            return self._write_file(content, file_path)

        except Exception as e:
            logger.error(
                f"Failed to generate view file for {view_data['name']}: {str(e)}"
            )
            raise
