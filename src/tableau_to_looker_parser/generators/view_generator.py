"""
View LookML generator.
"""

from typing import Dict, List, Set
import logging
from types import SimpleNamespace

from .base_generator import BaseGenerator
from ..converters.ast_to_lookml_converter import ASTToLookMLConverter
from ..models.ast_schema import ASTNode

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

            logger.info(f"Generated {len(view_files)} view files")
            return view_files

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

        # Filter calculated fields for this specific table and convert AST to LookML
        table_calculated_fields = []
        for calc_field in all_calculated_fields:
            if calc_field.get("table_name") == actual_table_name:
                # Convert AST to LookML SQL using our converter
                converted_field = self._convert_calculated_field(calc_field, view_name)
                if converted_field:
                    table_calculated_fields.append(converted_field)

        # Build view data
        view_data = {
            "name": view_name,
            "table_name": table_ref,
            "dimensions": table_dimensions,
            "measures": table_measures,
            "calculated_fields": table_calculated_fields,
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

    def _convert_calculated_field(self, calc_field: Dict, table_context: str) -> Dict:
        """
        Convert a calculated field with AST to LookML format.

        Args:
            calc_field: Calculated field data with AST
            table_context: Table context for field references

        Returns:
            Dict with converted LookML field data
        """
        try:
            # Extract AST from calculation
            calculation = calc_field.get("calculation", {})
            ast_data = calculation.get("ast", {})

            if not ast_data:
                logger.warning(
                    f"No AST data found for calculated field: {calc_field.get('name')}"
                )
                return None

            # Convert dict to ASTNode object
            ast_node = ASTNode(**ast_data)

            # Convert AST to LookML SQL expression
            lookml_sql = self.ast_converter.convert_to_lookml(ast_node, "TABLE")

            # Determine LookML type for measures with aggregation
            lookml_type = self._determine_lookml_type(calc_field, calculation)

            # Build LookML field definition
            converted_field = {
                "name": self._clean_name(calc_field.get("name", "")),
                "original_name": calc_field.get("original_name", ""),
                "field_type": calc_field.get("field_type", "dimension"),
                "role": calc_field.get("role", "dimension"),
                "datatype": calc_field.get("datatype", "string"),
                "sql": lookml_sql,
                "original_formula": calculation.get("original_formula", ""),
                "description": f"Calculated field: {self._normalize_formula_for_description(calculation.get('original_formula', ''))}",
                "lookml_type": lookml_type,  # Add LookML type for template
            }

            logger.debug(
                f"Converted calculated field: {calc_field.get('name')} → {lookml_sql}"
            )
            return converted_field

        except Exception as e:
            logger.error(
                f"Failed to convert calculated field {calc_field.get('name')}: {str(e)}"
            )
            return None

    def _determine_lookml_type(self, calc_field: Dict, calculation: Dict) -> str:
        """
        Determine the appropriate LookML type for a calculated field.

        For measures, this detects if the formula already contains aggregation
        and returns 'number' instead of 'sum' to avoid double aggregation.

        Args:
            calc_field: Calculated field data
            calculation: Calculation metadata

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
            # For measures, check if formula already contains aggregation
            original_formula = calculation.get("original_formula", "")
            requires_aggregation = calculation.get("requires_aggregation", False)

            # Check if formula contains aggregation functions
            agg_functions = ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "MEDIAN("]
            has_aggregation = any(
                func in original_formula.upper() for func in agg_functions
            )

            if has_aggregation or requires_aggregation:
                # Already aggregated - use number type to avoid double aggregation
                return "number"
            else:
                # Not aggregated - use sum type
                return "sum"

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
            # Prepare template context
            context = {
                "view": SimpleNamespace(**view_data),
                "view_name": self._clean_name(view_data["name"]),
                "table_name": self._format_table_name(view_data["table_name"]),
                "dimensions": view_data["dimensions"],
                "measures": view_data["measures"],
                "calculated_fields": view_data["calculated_fields"],
                "has_dimensions": len(view_data["dimensions"]) > 0,
                "has_measures": len(view_data["measures"]) > 0,
                "has_calculated_fields": len(view_data["calculated_fields"]) > 0,
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
