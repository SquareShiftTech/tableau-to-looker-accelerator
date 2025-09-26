"""
View LookML generator.
"""

from typing import Dict, List, Set
import logging
from types import SimpleNamespace

from .base_generator import BaseGenerator

# from .parameter_generator import generate_looker_parameters
from ..converters.ast_to_lookml_converter import ASTToLookMLConverter
from ..models.ast_schema import ASTNode, NodeType

logger = logging.getLogger(__name__)


class ViewGenerator(BaseGenerator):
    """Generator for view.lkml files."""

    def __init__(self, template_engine=None):
        """Initialize the view generator with AST converter."""
        super().__init__(template_engine)
        self.ast_converter = ASTToLookMLConverter()

    def generate_views(self, migration_data: Dict, output_dir: str) -> List[str]:
        """
        Generate all view.lkml files for a migration.

        Args:
            migration_data: Complete migration data
            output_dir: Directory to write files to

        Returns:
            List of paths to generated view files
        """
        try:
            view_files = []
            view_mappings = []
            all_dimensions = migration_data.get("dimensions", [])
            all_measures = migration_data.get("measures", [])
            all_calculated_fields = migration_data.get("calculated_fields", [])

            # Create AST converter with calculated fields context
            self.ast_converter = ASTToLookMLConverter(all_calculated_fields)

            # Determine which views need to be generated
            view_names_needed = self._determine_view_names(migration_data)

            # Generate view file for each needed view
            for view_name in view_names_needed:
                view_file, view_mapping = self._generate_single_view(
                    view_name,
                    migration_data,
                    all_dimensions,
                    all_measures,
                    all_calculated_fields,
                    output_dir,
                )
                if view_file:
                    view_files.append(view_file)
                    view_mapping_formmated = {view_name: view_mapping}
                    view_mappings.append(view_mapping_formmated)

            logger.info(f"Generated {len(view_files)} view files")
            return view_files, view_mappings

        except Exception as e:
            logger.error(f"Failed to generate view files: {str(e)}")
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
        """Generate a single view file."""
        actual_table, table_ref = self._resolve_view_table(view_name, migration_data)

        if not actual_table:
            logger.warning(f"Could not resolve table for view: {view_name}")
            return None

        actual_table_name = actual_table["name"]

        table_dimensions = [
            dim
            for dim in all_dimensions
            if dim.get("table_name") == actual_table_name
            and not dim.get("is_internal", False)
        ]

        # Filter measures for this specific table
        table_measures = [
            measure
            for measure in all_measures
            if measure.get("table_name") == actual_table_name
            and not measure.get("is_internal", False)
        ]

        # Filter calculated fields for this specific table and convert AST to LookML, excluding internal fields

        table_calculated_fields = []
        table_calculated_dimensions = []

        table_name = self._format_table_name(table_ref)

        # NEW: Placeholder for derived table SQL
        derived_table_sql = None
        is_derived_table = False

        if actual_table.get("relation_type") in ["custom_sql", "Custom_Sql"]:
            derived_table_sql = actual_table.get("sql_query")  # The SQL query
            is_derived_table = True

        for calc_field in all_calculated_fields:
            # Include calculated fields with matching table_name or with placeholder "__UNASSIGNED_TABLE__"
            if (
                calc_field.get("table_name") == actual_table_name
                or calc_field.get("table_name") == "__UNASSIGNED_TABLE__"
            ):
                # Convert AST to LookML SQL using our converter
                if calc_field.get("name") == "suag_ris_percent":
                    print("here")

                # Build dictionary lookup for all calculated fields
                all_calculated_fields_dict = {}
                for field in all_calculated_fields:
                    field_name = field.get("original_name", "")
                    if field_name:
                        all_calculated_fields_dict[field_name] = field

                converted_field = self._convert_calculated_field(
                    calc_field, view_name, all_calculated_fields_dict
                )
                if converted_field.get("name") == "rptmth_copy":
                    print("here")
                if converted_field:
                    # NEW: Check if this field defines a derived table
                    ast_data = calc_field.get("calculation", {}).get("ast", {})
                    if ast_data:
                        try:
                            ast_node = ASTNode(**ast_data)
                            if ast_node.node_type == NodeType.DERIVED_TABLE:
                                derived_table_sql = (
                                    self.ast_converter.convert_to_lookml(
                                        ast_node, table_name=table_name
                                    )
                                )
                                is_derived_table = True
                                logger.info(
                                    f"Derived table detected in {calc_field['name']} for view {view_name}"
                                )
                        except Exception as e:
                            logger.warning(
                                f"Failed to check derived table for field {calc_field.get('name')}: {e}"
                            )

                    if converted_field.get("two_step_pattern"):
                        table_calculated_dimensions.append(converted_field["dimension"])
                        table_calculated_fields.append(converted_field["measure"])
                    else:
                        table_calculated_fields.append(converted_field)

        # Build view data - combine dimensions including hidden ones from calculated fields
        visible_dimensions = [
            dim for dim in table_calculated_dimensions if not dim.get("hidden")
        ]
        all_dimensions = (
            table_dimensions
            + visible_dimensions  # Only visible dimensions from calculated field two-step pattern
        )

        # Extract parameters for this view
        view_parameters = []
        if "parameters" in migration_data:
            view_parameters = migration_data["parameters"]

        view_data = {
            "name": view_name,
            "table_name": table_ref,
            "dimensions": all_dimensions,  # All dimensions including hidden ones from calculated fields
            "measures": table_measures,
            "calculated_fields": table_calculated_fields,
            "calculated_dimensions": table_calculated_dimensions,
            "derived_table_sql": derived_table_sql,  # <-- NEW
            "is_derived_table": is_derived_table,  # <-- NEW
            "parameters": view_parameters,  # <-- NEW: Add parameters
        }

        return self._create_view_file(view_data, output_dir), view_data

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

    def _convert_calculated_field(
        self,
        calc_field: Dict,
        table_context: str,
        all_calculated_fields_dict: Dict[str, Dict] = None,
    ) -> Dict:
        """
        Convert a calculated field with AST to LookML format.

        For calculated field measures, implements two-step pattern:
        1. Generate hidden dimension for row-level calculation
        2. Generate measure that aggregates the dimension

        Args:
            calc_field: Calculated field data with AST
            table_context: Table context for field references

        Returns:
            Dict with converted LookML field data (may contain both dimension and measure)
        """
        try:
            # Extract AST from calculation
            calculation = calc_field.get("calculation", {})
            ast_data = calculation.get("ast", {})

            if calc_field.get("name") == "om_officeto_numeric":
                print("here")

            if not ast_data:
                logger.warning(
                    f"No AST data found for calculated field: {calc_field.get('name')}"
                )
                return self._create_fallback_lookml_field(
                    calc_field,
                    "No AST data available - formula parsing may have failed",
                )

            # Convert dict to ASTNode object
            ast_node = ASTNode(**ast_data)
            if ast_node.node_type == NodeType.DERIVED_TABLE:
                lookml_sql = (
                    f"${{TABLE}}.{ast_node.properties.get('derived_field_alias')}"
                )
            else:
                # Convert AST to LookML SQL expression
                lookml_sql = self.ast_converter.convert_to_lookml(ast_node, "TABLE")

            # Check if this is a fallback AST node created due to parsing failure
            is_fallback = (
                ast_node.properties
                and ast_node.properties.get("migration_status") == "MANUAL_REQUIRED"
            )

            # Check if this is a measure that needs two-step pattern
            role = calc_field.get("role", "dimension")
            if role == "measure" and self._needs_two_step_pattern(
                calc_field, calculation
            ):
                return self._create_two_step_pattern(
                    calc_field, lookml_sql, calculation, all_calculated_fields_dict
                )

            # Standard single field conversion for dimensions and simple measures
            # Determine LookML type for measures with aggregation
            lookml_type = self._determine_lookml_type(
                calc_field, calculation, all_calculated_fields_dict
            )

            # Extract calculation ID from original_name to sync with dashboard references
            field_name = self._extract_calculation_name(calc_field)

            # Build LookML field definition
            converted_field = {
                "name": field_name,  # Use calculation ID instead of friendly name
                "original_name": calc_field.get("original_name", ""),
                "field_type": calc_field.get("field_type", "dimension"),
                "role": calc_field.get("role", "dimension"),
                "datatype": calc_field.get("datatype", "string"),
                "sql": lookml_sql,
                "original_formula": calculation.get("original_formula", ""),
                "description": f"Calculated field: {self._normalize_formula_for_description(calculation.get('original_formula', ''))}",
                "lookml_type": lookml_type,  # Add LookML type for template
                "datasource_id": calc_field.get("datasource_id", ""),
                "local_name": calc_field.get("local_name", ""),
            }

            # Add migration metadata for fallback fields
            if is_fallback:
                converted_field.update(
                    {
                        "migration_error": True,
                        "migration_comment": f"""MIGRATION_ERROR: Could not parse Tableau formula
ORIGINAL_FORMULA: {calculation.get("original_formula", "UNKNOWN")}
PARSE_ERROR: {ast_node.properties.get("parse_error", "Unknown error")}
TODO: Manual migration required - please convert this formula manually""",
                    }
                )

            logger.debug(
                f"Converted calculated field: {calc_field.get('name')} → {lookml_sql}"
            )
            return converted_field

        except Exception as e:
            logger.error(
                f"Failed to convert calculated field {calc_field.get('name')}: {str(e)}"
            )

            # Create fallback field instead of failing completely
            return self._create_fallback_lookml_field(calc_field, str(e))

    def _create_fallback_lookml_field(
        self, calc_field: Dict, error_message: str
    ) -> Dict:
        """
        Create a fallback LookML field when conversion fails.

        This ensures LookML generation continues even with problematic calculated fields.
        The fallback includes migration comments with the original formula.
        """
        calculation = calc_field.get("calculation", {})
        original_formula = calculation.get("original_formula", "UNKNOWN_FORMULA")
        # Use calculation ID for field name to sync with dashboard references
        field_name = self._extract_calculation_id(calc_field)

        # Create safe fallback field
        fallback_field = {
            "name": field_name,  # Use calculation ID instead of friendly name
            "original_name": calc_field.get("original_name", ""),
            "field_type": calc_field.get("field_type", "dimension"),
            "role": calc_field.get("role", "dimension"),
            "sql": "'MIGRATION_REQUIRED'",  # Safe SQL placeholder
            "original_formula": original_formula,
            "description": "MIGRATION ERROR - Manual conversion required",
            "lookml_type": "string",  # Safe default type
            "migration_error": True,  # Flag for template to add comments
            "migration_comment": f"""MIGRATION_ERROR: Could not convert calculated field
ORIGINAL_FORMULA: {original_formula}
CONVERSION_ERROR: {error_message}
TODO: Manual migration required - please convert this formula manually""",
        }

        logger.warning(f"Created fallback LookML field for: {field_name}")
        return fallback_field

    def _determine_lookml_type(
        self,
        calc_field: Dict,
        calculation: Dict,
        all_calculated_fields_dict: Dict[str, Dict] = None,
    ) -> str:
        """
        Determine the appropriate LookML type for a calculated field.

        Rules:
        1. Has explicit aggregation attribute → return that aggregation
        2. Formula contains aggregation functions → number
        3. References fields that contain aggregation → number
        4. Default case → sum

        Args:
            calc_field: Calculated field data
            calculation: Calculation metadata
            all_calculated_fields_dict: Dictionary lookup of all calculated fields by name

        Returns:
            str: LookML type ('sum', 'number', 'string', 'yesno')
        """
        field_role = calc_field.get("role", "dimension")

        if field_role == "dimension":
            # For dimensions, use appropriate type based on datatype
            datatype = calc_field.get("datatype", "string")
            if datatype == "boolean":
                return "yesno"
            elif datatype in ["integer", "real"]:
                return "number"
            else:
                return "string"
        else:
            # For measures, check aggregation logic
            aggregation = calc_field.get("aggregation")
            original_formula = calculation.get("original_formula", {})

            # Rule 1: Has explicit aggregation attribute → return that aggregation
            if aggregation:
                return aggregation.lower()

            # Rule 2: Formula contains aggregation functions → number
            agg_functions = ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "MEDIAN("]
            has_aggregation = any(
                func in original_formula.upper() for func in agg_functions
            )

            if has_aggregation:
                return "number"

            # Rule 3: References fields that contain aggregation → number
            if all_calculated_fields_dict and self._references_aggregated_fields(
                original_formula, all_calculated_fields_dict
            ):
                return "number"

            # Rule 4: Default case → sum
            return "sum"

    def _references_aggregated_fields(
        self, formula: str, all_calculated_fields_dict: Dict[str, Dict]
    ) -> bool:
        """
        Check if formula references fields that contain aggregation.

        Args:
            formula: The formula to check
            all_calculated_fields_dict: Dictionary lookup of all calculated fields by name

        Returns:
            bool: True if formula references fields containing aggregation
        """
        import re

        # Extract field references from square brackets
        field_refs = re.findall(r"\[([^\]]+)\]", formula)

        # Check if any referenced field contains aggregation
        agg_functions = ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "MEDIAN("]

        for field_ref in field_refs:
            # Clean the field reference (remove brackets)
            # clean_field_name = field_ref.strip("[]")

            # Look up the field in our dictionary
            referenced_field = all_calculated_fields_dict.get(f"[{field_ref}]")

            if referenced_field:
                # Get the referenced field's formula
                referenced_formula = referenced_field.get("calculation", {}).get(
                    "original_formula", ""
                )

                # Check if the referenced field contains aggregation
                if any(func in referenced_formula.upper() for func in agg_functions):
                    return True

        return False

    def _needs_two_step_pattern(self, calc_field: Dict, calculation: Dict) -> bool:
        """
        Determine if a calculated field measure needs the two-step pattern.

        Two-step pattern is needed when:
        1. Field role is 'measure'
        2. Formula contains field references
        3. Formula does not already contain aggregation functions

        Args:
            calc_field: Calculated field data
            calculation: Calculation metadata

        Returns:
            bool: True if two-step pattern is needed
        """
        # Only applies to measures
        if calc_field.get("role") != "measure":
            return False

        # dependencies = calculation.get("dependencies", [])

        # If no field references, no need for two-step pattern
        # if not dependencies:
        # return False

        # If formula already contains aggregation, no need for two-step pattern
        # agg_functions = ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "MEDIAN("]
        # has_aggregation = any(
        #    func in original_formula.upper() for func in agg_functions
        # )
        has_aggregation = False

        if has_aggregation:
            return False

        # Measure with field references but no aggregation = needs two-step pattern
        logger.debug(
            f"Calculated field measure '{calc_field.get('name')}' needs two-step pattern"
        )
        return True

    def _create_two_step_pattern(
        self,
        calc_field: Dict,
        lookml_sql: str,
        calculation: Dict,
        all_calculated_fields_dict: Dict[str, Dict] = None,
    ) -> Dict:
        """
        Create two-step pattern: hidden dimension + aggregated measure.

        Args:
            calc_field: Original calculated field data
            lookml_sql: Converted LookML SQL expression
            calculation: Calculation metadata

        Returns:
            Dict with two-step pattern fields (dimension and measure)
        """
        # Extract calculation ID to sync with dashboard references
        calc_name = self._extract_calculation_name(calc_field)
        original_formula = calculation.get("original_formula", "")


        # Create hidden dimension for row-level calculation
        dimension_field = {
            "name": f"{calc_name}_calc",  # Use calculation ID + _calc suffix
            "original_name": calc_field.get("original_name", ""),
            "field_type": "dimension",
            "role": "dimension",
            "datatype": calc_field.get(
                "datatype", "real"
            ),  # Usually numeric for measures
            "sql": lookml_sql,
            "original_formula": original_formula,
            "description": f"Row-level calculation for {calc_name}: {self._normalize_formula_for_description(original_formula)}",
            "lookml_type": "number",
            "hidden": True,  # Hide the calculation dimension
            "is_two_step_dimension": True,  # Flag for template
            "datasource_id": calc_field.get("datasource_id", ""),
        }

        # Create measure that aggregates the dimension - use the exact calculation ID to match dashboard
        measure_field = {
            "name": calc_name,  # Use exact calculation ID as dashboard references it
            "original_name": calc_field.get("original_name", ""),
            "field_type": "measure",
            "role": "measure",
            "datatype": calc_field.get("datatype", "real"),
            "sql": f"${{{calc_name}_calc}}",  # Reference the hidden dimension
            "original_formula": original_formula,
            "description": f"Calculated field: {self._normalize_formula_for_description(original_formula)}",
            "lookml_type": self._determine_lookml_type(
                calc_field, calculation, all_calculated_fields_dict
            ),
            "is_two_step_measure": True,  # Flag for template
            "references_dimension": f"{calc_name}_calc",  # Reference to dimension
            "default_format": calc_field.get("default_format", ""),
        }

        logger.debug(
            f"Created two-step pattern for '{calc_name}': dimension '{calc_name}_calc' + measure '{calc_name}'"
        )

        return {
            "two_step_pattern": True,
            "dimension": dimension_field,
            "measure": measure_field,
        }

    def _extract_calculation_name(self, calc_field: Dict) -> str:
        """
        Extract calculation ID from original_name field to sync with dashboard references.

        Args:
            calc_field: Calculated field data with original_name

        Returns:
            str: Calculation ID (e.g., 'calculation_1181350527289110528')
        """
        return self._clean_name(calc_field.get("name", "unknown_calc"))

    def _extract_calculation_id(self, calc_field: Dict) -> str:
        """
        Extract calculation ID from original_name field to sync with dashboard references.

        Args:
            calc_field: Calculated field data with original_name

        Returns:
            str: Calculation ID (e.g., 'calculation_1181350527289110528')
        """
        original_name = calc_field.get("original_name", "")
        if not original_name:
            # Fallback to friendly name if no original_name
            return self._clean_name(calc_field.get("name", "unknown_calc"))

        # Extract ID from format like "[Calculation_1181350527289110528]"
        if original_name.startswith("[Calculation_") and original_name.endswith("]"):
            calc_name = original_name[1:-1]  # Remove brackets
            calc_name = calc_name.lower()  # Convert to lowercase for consistency
            return calc_name
        elif original_name.startswith("[") and original_name.endswith("]"):
            # Handle other calculation formats, convert to lowercase and clean
            calc_name = original_name[1:-1]  # Remove brackets
            calc_name = self._clean_name(calc_name)
            return calc_name
        else:
            # Fallback if format is unexpected
            logger.warning(f"Unexpected original_name format: {original_name}")
            return self._clean_name(calc_field.get("name", "unknown_calc"))

    def _normalize_formula_for_description(self, formula: str) -> str:
        """Normalize multi-line formulas for use in descriptions."""
        if not formula:
            return formula

        import re

        # Replace line breaks with spaces and normalize whitespace
        single_line = re.sub(r"\s*\n\s*", " ", formula.strip())
        # Normalize multiple spaces to single space
        single_line = re.sub(r"\s+", " ", single_line)
        return single_line

    def _format_table_name(self, table_name: str) -> str:
        """Format table name from [schema].[table] to `schema.table`."""
        if not table_name:
            return table_name

        # Remove square brackets and replace with backticks
        # Convert [schema].[table] to `schema.table`
        formatted = table_name.replace("[", "").replace("]", "").replace(".", ".")
        return f"`{formatted}`"

    def _create_view_file(self, view_data: Dict, output_dir: str) -> str:
        """Create a single view file from view data."""
        try:
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
                "derived_table_sql": view_data.get("derived_table_sql"),  # <-- NEW
                "is_derived_table": view_data.get("is_derived_table", False),  # <-- NEW
                "parameters": view_data.get(
                    "parameters", []
                ),  # <-- NEW: Add parameters
                "has_parameters": len(view_data.get("parameters", []))
                > 0,  # <-- NEW: Check if parameters exist
            }

            content = self.template_engine.render_template("basic_view.j2", context)
            output_path = self._ensure_output_dir(output_dir)
            view_filename = f"{self._clean_name(view_data['name'])}{self.view_extension}{self.lookml_extension}"
            file_path = output_path / view_filename

            return self._write_file(content, file_path)

        except Exception as e:
            logger.error(
                f"Failed to generate view file for {view_data['name']}: {str(e)}"
            )
            raise
