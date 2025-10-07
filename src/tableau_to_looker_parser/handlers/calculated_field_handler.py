"""
Calculated Field Handler - Processes Tableau calculated fields with AST generation.
Integrates with existing handler system and formula parser.
"""

import logging
from typing import Dict, List, Optional

from .base_handler import BaseHandler
from ..converters.formula_parser import FormulaParser
from ..models.ast_schema import CalculatedField
from ..models.parser_models import FunctionRegistry, OperatorRegistry
from ..core.field_name_mapper import field_name_mapper

logger = logging.getLogger(__name__)


class CalculatedFieldHandler(BaseHandler):
    """
    Handler for Tableau calculated fields.

    Handles:
    - Parsing Tableau formulas into AST
    - Converting calculated dimensions and measures to JSON format
    - Field dependency analysis
    - Complexity assessment and confidence scoring

    Does NOT handle XML parsing - that's XMLParser's responsibility.
    """

    def __init__(
        self,
        function_registry: Optional[FunctionRegistry] = None,
        operator_registry: Optional[OperatorRegistry] = None,
    ):
        """
        Initialize the calculated field handler.

        Args:
            function_registry: Registry of supported functions (uses default if None)
            operator_registry: Registry of supported operators (uses default if None)
        """
        super().__init__()
        self.parser = FormulaParser(function_registry, operator_registry)
        self.used_field_names = set()  # Track used field names to avoid duplicates

        logger.info("CalculatedFieldHandler initialized with formula parser")

    def can_handle(self, data: Dict) -> float:
        """
        Determine if this handler can process the raw data.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            float: Confidence score between 0.0 and 1.0
        """
        # Must have a calculation formula to be a calculated field
        if not data.get("calculation"):
            return 0.0

        # Must be a dimension or measure with a role
        role = data.get("role", "")
        if role not in ["dimension", "measure"]:
            return 0.0

        # Check if we have the minimum required fields
        name = data.get("name", "")
        if not name:
            return 0.0

        calculation = data.get("calculation", "")
        if not calculation.strip():
            return 0.0

        # Higher confidence for common calculated field patterns
        confidence = 0.8

        # Increase confidence for common formula patterns
        formula = calculation.upper()
        if any(
            pattern in formula for pattern in ["IF", "CASE", "SUM(", "COUNT(", "AVG("]
        ):
            confidence = 0.9

        # Increase confidence for field references
        if "[" in calculation and "]" in calculation:
            confidence = 0.95

        # Perfect confidence if we have all expected metadata
        if data.get("datatype") and data.get("aggregation"):
            confidence = 1.0

        if data.get("is_derived"):
            confidence = 1.0

        logger.debug(f"Calculated field confidence for '{name}': {confidence}")
        return confidence

    def convert_to_json(
        self, data: Dict, field_table_mapping: Optional[Dict[str, str]] = None
    ) -> Dict:
        """
        Convert raw calculated field data to schema-compliant JSON.

        Args:
            data: Raw data dict from XMLParser containing calculated field info
            field_table_mapping: Optional mapping from field names to table names

        Returns:
            Dict: Schema-compliant calculated field data with AST
        """
        # Extract basic field information
        # Prefer user-friendly caption/label over auto-generated Tableau ID
        user_caption = data.get("caption") or data.get("label")
        if user_caption == "Rolling 24":
            print(user_caption)
        if user_caption:
            # Use caption for clean field name (convert to snake_case for LookML compatibility)
            field_name = field_name_mapper.create_clean_name_from_caption(user_caption)
            display_name = user_caption
        else:
            # Fallback to Tableau's generated name
            field_name = data.get("name", "").strip("[]")
            display_name = field_name

        original_name = data.get("name", "")  # Always preserve Tableau's internal ID

        if field_name in self.used_field_names:
            # Field name already exists, add a suffix
            counter = 2
            original_field_name = field_name
            while field_name in self.used_field_names:
                field_name = f"{original_field_name}_{counter}"
                counter += 1
            logger.warning(
                f"Duplicate field name detected. Renamed '{original_field_name}' to '{field_name}'"
            )

        # Add the field name to our tracking set
        self.used_field_names.add(field_name)

        if field_name == "om_credit_app_numeric":
            print("here")

        # Register the field mapping if we have both original name and clean name
        if original_name and field_name:
            field_name_mapper.register_field(
                original_name, field_name, user_caption, is_calculated=True
            )
        role = data.get("role", "dimension")
        calculation = data.get("calculation", "")

        logger.info(f"Converting calculated field: {field_name}")

        # Parse the formula into AST
        parse_result = self.parser.parse_formula(
            formula=calculation, field_name=field_name, field_type=role
        )

        if not parse_result.success:
            # Parsing failed - create a fallback representation
            logger.warning(
                f"Failed to parse formula for {field_name}: {parse_result.error_message}"
            )

            return self._create_fallback_calculated_field(
                data=data,
                field_name=field_name,
                display_name=display_name,
                role=role,
                calculation=calculation,
                error=parse_result.error_message,
            )

        # Successfully parsed - use the calculated field from parser
        calculated_field = parse_result.calculated_field

        # Enhance with additional metadata from XML
        self._enhance_calculated_field_metadata(calculated_field, data)

        # Infer table_name if not provided by analyzing dependencies
        table_name = data.get("table_name")
        if not table_name:
            if calculated_field.dependencies:
                table_name = self._infer_table_from_dependencies(
                    calculated_field.dependencies, data, field_table_mapping
                )
            else:
                # Assign a special placeholder table_name to ensure generation
                table_name = "__UNASSIGNED_TABLE__"

        is_derived = data.get("is_derived", False)
        tableau_instance = None
        if is_derived:
            tableau_instance = data.get("tableau_instance")

        # Create the JSON representation
        result = {
            "name": field_name,
            "original_name": original_name,
            "role": role,
            "field_type": role,  # For compatibility
            "datatype": self._map_data_type(data.get("datatype", "string")),
            "default_format": data.get("default_format"),
            "tableau_instance": tableau_instance,
            "is_derived": is_derived,
            # Core calculated field data
            "calculation": {
                "original_formula": calculation,
                "ast": calculated_field.ast_root.model_dump(),
                "complexity": calculated_field.complexity,
                "dependencies": calculated_field.dependencies,
                "requires_aggregation": calculated_field.requires_aggregation,
                "is_deterministic": calculated_field.is_deterministic,
                "parse_confidence": calculated_field.parse_confidence,
            },
            # Additional metadata
            "table_name": table_name,
            "aggregation": data.get("aggregation", "none"),
            "default_aggregate": data.get("default_aggregate"),
            "number_format": data.get("number_format"),
            "label": display_name,
            "display_name": display_name,  # Explicit display name for UI
            "description": data.get("description"),
            "folder": data.get("folder"),
            "hidden": data.get("hidden", False),
            # Quality metrics
            "validation_errors": calculated_field.validation_errors,
            "warnings": calculated_field.warnings,
            # Handler metadata
            "metadata": {
                "handler": self.__class__.__name__,
                "confidence": self.can_handle(data),
                "ast_nodes_count": parse_result.ast_nodes_count or 0,
                "tokens_count": parse_result.tokens_count or 0,
            },
            "datasource_id": data.get("datasource_id"),
            "local_name": data.get("raw_name"),
        }

        logger.debug(
            f"Successfully converted calculated field {field_name} with {len(calculated_field.dependencies)} dependencies"
        )
        return result

    def _create_fallback_calculated_field(
        self,
        data: Dict,
        field_name: str,
        display_name: str,
        role: str,
        calculation: str,
        error: str,
    ) -> Dict:
        """
        Create a fallback representation when formula parsing fails.

        Args:
            data: Original field data
            field_name: Clean field name (snake_case)
            display_name: User-friendly display name
            role: Field role (dimension/measure)
            calculation: Original formula
            error: Parse error message

        Returns:
            Dict: Fallback calculated field representation
        """
        logger.info(f"Creating fallback representation for {field_name}")

        # Extract basic dependencies by looking for [Field Name] patterns
        dependencies = self._extract_basic_dependencies(calculation)

        return {
            "name": field_name,
            "original_name": data.get("name", ""),
            "role": role,
            "field_type": role,
            "datatype": self._map_data_type(data.get("datatype", "string")),
            # Fallback calculation data
            "calculation": {
                "original_formula": calculation,
                "ast": None,  # No AST available
                "complexity": "unknown",
                "dependencies": dependencies,
                "requires_aggregation": self._guess_aggregation_requirement(
                    calculation
                ),
                "is_deterministic": True,  # Conservative assumption
                "parse_confidence": 0.0,
                "parse_error": error,
            },
            # Additional metadata
            "table_name": data.get("table_name"),
            "aggregation": data.get("aggregation", "none"),
            "label": display_name,
            "display_name": display_name,  # Explicit display name for UI
            "description": data.get("description"),
            "hidden": data.get("hidden", False),
            # Quality metrics
            "validation_errors": [f"Formula parsing failed: {error}"],
            "warnings": ["Manual review required for this calculated field"],
            # Handler metadata
            "metadata": {
                "handler": self.__class__.__name__,
                "confidence": 0.3,  # Low confidence for fallback
                "fallback": True,
                "requires_manual_review": True,
            },
        }

    def _enhance_calculated_field_metadata(
        self, calculated_field: CalculatedField, data: Dict
    ):
        """
        Enhance the calculated field with additional metadata from XML.

        Args:
            calculated_field: Parsed calculated field to enhance
            data: Original XML data
        """
        # Add table association if available
        if data.get("table_name"):
            calculated_field.properties = calculated_field.properties or {}
            calculated_field.properties["table_name"] = data["table_name"]

        # Add aggregation info
        if data.get("aggregation"):
            calculated_field.properties = calculated_field.properties or {}
            calculated_field.properties["aggregation"] = data["aggregation"]

        # Update field type based on aggregation
        if data.get("aggregation") in ["sum", "count", "avg", "min", "max"]:
            calculated_field.field_type = "measure"
            calculated_field.requires_aggregation = True

    def _map_data_type(self, tableau_datatype: str) -> str:
        """
        Map Tableau data types to standard types.

        Args:
            tableau_datatype: Tableau data type string

        Returns:
            str: Mapped data type
        """
        type_mapping = {
            "string": "string",
            "integer": "integer",
            "real": "real",
            "boolean": "boolean",
            "date": "date",
            "datetime": "datetime",
            "number": "real",  # Generic number -> real
        }

        return type_mapping.get(tableau_datatype.lower(), "string")

    def _extract_basic_dependencies(self, formula: str) -> List[str]:
        """
        Extract field dependencies using basic pattern matching.
        Fallback when AST parsing fails.

        Args:
            formula: Formula string to analyze

        Returns:
            List[str]: List of field names referenced
        """
        import re

        # Find all [Field Name] patterns
        pattern = r"\[([^\]]+)\]"
        matches = re.findall(pattern, formula)

        # Clean up field names and remove duplicates
        dependencies = []
        for match in matches:
            clean_name = match.strip().lower().replace(" ", "_")
            if clean_name and clean_name not in dependencies:
                dependencies.append(clean_name)

        return sorted(dependencies)

    def _guess_aggregation_requirement(self, formula: str) -> bool:
        """
        Guess if formula requires aggregation based on function patterns.

        Args:
            formula: Formula string to analyze

        Returns:
            bool: True if likely requires aggregation
        """
        formula_upper = formula.upper()

        # Common aggregation function patterns
        agg_patterns = [
            "SUM(",
            "COUNT(",
            "AVG(",
            "MIN(",
            "MAX(",
            "MEDIAN(",
            "STDEV(",
            "VAR(",
            "PERCENTILE(",
        ]

        return any(pattern in formula_upper for pattern in agg_patterns)

    def get_field_dependencies(self, data: Dict) -> List[str]:
        """
        Get field dependencies for a calculated field.

        Args:
            data: Raw calculated field data

        Returns:
            List[str]: List of field names this calculated field depends on
        """
        calculation = data.get("calculation", "")
        if not calculation:
            return []

        # Try to parse and get dependencies from AST
        parse_result = self.parser.parse_formula(calculation)
        if parse_result.success and parse_result.calculated_field:
            return parse_result.calculated_field.dependencies

        # Fallback to basic pattern matching
        return self._extract_basic_dependencies(calculation)

    def validate_calculated_field(self, data: Dict) -> Dict:
        """
        Validate a calculated field and return validation results.

        Args:
            data: Raw calculated field data

        Returns:
            Dict: Validation results with errors and warnings
        """
        errors = []
        warnings = []

        # Basic validation
        if not data.get("name"):
            errors.append("Calculated field missing name")

        calculation = data.get("calculation", "")
        if not calculation.strip():
            errors.append("Calculated field missing formula")
        else:
            # Try to parse the formula
            parse_result = self.parser.parse_formula(calculation)
            if not parse_result.success:
                errors.append(f"Formula parsing failed: {parse_result.error_message}")
            elif parse_result.calculated_field:
                # Add any parsing warnings
                warnings.extend(parse_result.calculated_field.warnings)

        # Role validation
        role = data.get("role", "")
        if role not in ["dimension", "measure"]:
            warnings.append(f"Unusual field role: {role}")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "confidence": self.can_handle(data) if len(errors) == 0 else 0.0,
        }

    def get_supported_functions(self) -> List[str]:
        """
        Get list of supported Tableau functions.

        Returns:
            List[str]: List of supported function names
        """
        return list(self.parser.function_registry.functions.keys())

    def get_complexity_metrics(self, formula: str) -> Dict:
        """
        Get complexity metrics for a formula.

        Args:
            formula: Formula to analyze

        Returns:
            Dict: Complexity metrics
        """
        parse_result = self.parser.parse_formula(formula)

        if not parse_result.success or not parse_result.calculated_field:
            return {"level": "unknown", "score": 0, "error": parse_result.error_message}

        complexity = self.parser._analyze_complexity(
            parse_result.calculated_field.ast_root
        )
        return {
            "level": complexity.level,
            "score": complexity.score,
            "factors": complexity.factors,
            "depth": complexity.depth,
            "node_count": complexity.node_count,
            "function_count": complexity.function_count,
            "conditional_count": complexity.conditional_count,
        }

    def _infer_table_from_dependencies(
        self,
        dependencies: List[str],
        data: Dict,
        field_table_mapping: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """
        Infer table name for calculated field based on its field dependencies.

        Since calculated fields reference other fields, we can infer which table
        the calculated field belongs to by looking at the tables of its dependencies.

        Args:
            dependencies: List of field names this calculated field depends on
            data: Original field data (may contain context)
            field_table_mapping: Mapping from field names to table names from migration context

        Returns:
            Inferred table name or None
        """
        if not dependencies:
            return None

        logger.debug(
            f"Inferring table for calculated field with dependencies: {dependencies}"
        )

        # Use dynamic field-to-table mapping if available
        if field_table_mapping:
            tables_found = set()
            for dep in dependencies:
                # Try exact match first
                table_name = field_table_mapping.get(dep)
                if not table_name:
                    # Try case-insensitive match
                    for field_name, tbl_name in field_table_mapping.items():
                        if field_name.lower() == dep.lower():
                            table_name = tbl_name
                            break

                # If no direct match, try qualified names (table.field)
                if not table_name:
                    # Look for qualified field names that match our dependency
                    for field_name, tbl_name in field_table_mapping.items():
                        if "." in field_name:
                            # Extract the field part from qualified name
                            qualified_field = field_name.split(".", 1)[1]
                            if qualified_field.lower() == dep.lower():
                                table_name = tbl_name
                                break

                if table_name:
                    tables_found.add(table_name)

            # If all dependencies point to the same table, use that table
            if len(tables_found) == 1:
                inferred_table = tables_found.pop()
                logger.debug(f"Inferred table '{inferred_table}' from field mapping")
                return inferred_table
            elif len(tables_found) > 1:
                # Multiple tables - use the most common one or first alphabetically for consistency
                inferred_table = sorted(tables_found)[0]
                logger.warning(
                    f"Calculated field dependencies span multiple tables: {tables_found}. Using '{inferred_table}'"
                )
                return inferred_table

        # No hardcoded patterns - rely purely on dynamic inference
        # If field mapping didn't provide a table, we can't reliably infer one

        # If we can't infer, log a warning and return None
        logger.warning(
            f"Could not infer table for calculated field with dependencies: {dependencies}"
        )
        return None
