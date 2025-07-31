import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from tableau_to_looker_parser.core.xml_parser import TableauXMLParser
from tableau_to_looker_parser.core.xml_parser_v2 import TableauXMLParserV2
from tableau_to_looker_parser.core.plugin_registry import PluginRegistry
from tableau_to_looker_parser.handlers.base_handler import BaseHandler
from tableau_to_looker_parser.handlers.relationship_handler import RelationshipHandler
from tableau_to_looker_parser.handlers.connection_handler import ConnectionHandler
from tableau_to_looker_parser.handlers.dimension_handler import DimensionHandler
from tableau_to_looker_parser.handlers.measure_handler import MeasureHandler
from tableau_to_looker_parser.handlers.parameter_handler import ParameterHandler
from tableau_to_looker_parser.handlers.calculated_field_handler import (
    CalculatedFieldHandler,
)


class MigrationEngine:
    """Orchestrates the entire Tableau to LookML conversion process.

    Manages the conversion pipeline:
    1. XML parsing
    2. Data extraction using handlers
    3. JSON output generation
    4. LookML generation (future)
    """

    def __init__(self, use_v2_parser: bool = True):
        """Initialize migration engine.

        Args:
            use_v2_parser: If True, uses enhanced metadata-first parser (default: True)
        """
        self.logger = logging.getLogger(__name__)
        self.plugin_registry = PluginRegistry()
        self.use_v2_parser = use_v2_parser

        # Register default handlers
        self.register_handler(RelationshipHandler(), priority=1)
        self.register_handler(ConnectionHandler(), priority=2)
        self.register_handler(DimensionHandler(), priority=3)
        self.register_handler(MeasureHandler(), priority=4)
        self.register_handler(ParameterHandler(), priority=5)
        self.register_handler(
            CalculatedFieldHandler(), priority=6
        )  # After regular fields

    def register_handler(self, handler: BaseHandler, priority: int = 100) -> None:
        """Register a handler with the engine.

        Args:
            handler: Handler instance to register
            priority: Priority level (lower = higher priority)
        """
        self.plugin_registry.register_handler(handler, priority)

    def migrate_file(self, tableau_file: str, output_dir: str) -> Dict[str, Any]:
        """Convert a Tableau workbook to LookML.

        Args:
            tableau_file: Path to .twb or .twbx file
            output_dir: Directory to write output files

        Returns:
            Dict containing tables, relationships, and other extracted data

        Raises:
            FileNotFoundError: If tableau_file doesn't exist
            ValueError: If file is not a .twb or .twbx
            MigrationError: If conversion fails
        """
        # Validate inputs
        tableau_path = Path(tableau_file)
        if not tableau_path.exists():
            raise FileNotFoundError(f"Tableau file not found: {tableau_file}")

        if tableau_path.suffix.lower() not in [".twb", ".twbx"]:
            raise ValueError(f"Invalid file type: {tableau_path.suffix}")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        try:
            # Parse workbook - use v2 parser by default for enhanced field coverage
            if self.use_v2_parser:
                parser = TableauXMLParserV2()
                self.logger.info(
                    "Using enhanced XML Parser v2 (metadata-first approach)"
                )
            else:
                parser = TableauXMLParser()
                self.logger.info("Using legacy XML Parser v1")

            if tableau_path.suffix.lower() == ".twb":
                root = parser._parse_twb_file(tableau_path)
            else:
                root = parser._parse_twbx_file(tableau_path)

            # Initialize result structure
            result = {
                "metadata": {
                    "source_file": str(tableau_path),
                    "output_dir": str(output_path),
                },
                "tables": [],
                "relationships": [],
                "connections": [],
                "dimensions": [],
                "measures": [],
                "parameters": [],
                "calculated_fields": [],
            }

            # Process with handlers using clean architecture
            self.logger.info("Starting workbook processing")

            # Get all elements from parser - v2 provides enhanced field coverage
            if self.use_v2_parser:
                elements = parser.get_all_elements_enhanced(root)
            else:
                elements = parser.get_all_elements(root)
            self.logger.info(f"Found {len(elements)} elements to process")

            # Build field-to-table mapping for calculated field inference
            # V2 parser provides more accurate mappings from metadata-records
            field_table_mapping = self._build_field_table_mapping(elements)

            # Process each element through handlers
            for element in elements:
                if not element.get("data"):  # Skip None values
                    continue

                element_data = element["data"]
                element_name = element_data.get("name", "unnamed")
                self.logger.info(f"Processing {element['type']}: {element_name}")

                handled = False
                for handler in self.plugin_registry.get_handlers_by_priority():
                    confidence = handler.can_handle(element_data)
                    if confidence > 0:
                        self.logger.info(
                            f"Using {handler.__class__.__name__} (confidence: {confidence})"
                        )

                        # Provide field mapping context to calculated field handler
                        if handler.__class__.__name__ == "CalculatedFieldHandler":
                            json_data = handler.convert_to_json(
                                element_data, field_table_mapping
                            )
                        else:
                            json_data = handler.convert_to_json(element_data)

                        # Route to appropriate result category
                        # Check if this is a calculated field first
                        if handler.__class__.__name__ == "CalculatedFieldHandler":
                            result["calculated_fields"].append(json_data)
                        elif element["type"] == "measure":
                            result["measures"].append(json_data)
                        elif element["type"] == "dimension":
                            result["dimensions"].append(json_data)
                        elif element["type"] == "parameter":
                            result["parameters"].append(json_data)
                        elif element["type"] == "connection":
                            result["connections"].append(json_data)
                        elif element["type"] == "relationships":
                            # Special handling for relationships
                            result["tables"].extend(json_data.get("tables", []))
                            result["relationships"].extend(
                                json_data.get("relationships", [])
                            )

                        handled = True
                        break

                if not handled:
                    self.logger.warning(
                        f"No handler found for {element['type']}: {element_name}"
                    )

            # Save JSON output
            json_path = output_path / "processed_pipeline_output.json"
            with open(json_path, "w") as f:
                json.dump(result, f, indent=2)

            return result

        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}", exc_info=True)
            raise MigrationError(f"Failed to migrate {tableau_file}: {str(e)}")

    def _build_field_table_mapping(self, elements: List[Dict]) -> Dict[str, str]:
        """
        Build mapping from field names to table names for calculated field inference.

        Args:
            elements: List of parsed elements from XMLParser

        Returns:
            Dict mapping field names to their table names
        """
        field_table_mapping = {}

        # First, build mapping from main datasource elements
        for element in elements:
            if not element.get("data"):
                continue

            data = element["data"]
            element_type = element.get("type")

            # Only process dimensions and measures that have table assignments
            if element_type in ["dimension", "measure"]:
                field_name = data.get("raw_name", "").strip("[]")
                table_name = data.get("table_name")

                # Skip calculated fields (they don't help with inference)
                if data.get("calculation"):
                    continue

                if field_name and table_name:
                    field_table_mapping[field_name] = table_name

        # Additionally, try to extract fields from datasource-dependencies
        # This handles cases like Book6 where some fields are only defined in worksheet dependencies
        self._add_datasource_dependencies_to_mapping(field_table_mapping, elements)

        self.logger.debug(
            f"Built field-table mapping with {len(field_table_mapping)} entries"
        )
        return field_table_mapping

    def _add_datasource_dependencies_to_mapping(
        self, field_table_mapping: Dict[str, str], elements: List[Dict]
    ):
        """
        Extract additional field mappings from datasource-dependencies sections.

        This handles workbooks where fields are defined in worksheet dependencies
        rather than the main datasource section. Uses a generic approach
        that works for any dataset without hardcoded patterns.

        Args:
            field_table_mapping: Existing mapping to enhance
            elements: All parsed elements that may contain additional field references
        """
        try:
            # Strategy: Look for fields referenced in calculated field dependencies
            # that aren't in our current mapping, and infer their tables from context

            # Find all dependencies from calculated fields
            missing_fields = set()
            for element in elements:
                if not element.get("data"):
                    continue

                data = element["data"]
                if data.get("calculation"):
                    # This is a calculated field, check its dependencies
                    calc = data.get("calculation", "")
                    if calc:
                        # Extract field references like [Sales], [Revenue], etc.
                        import re

                        field_refs = re.findall(r"\[([^\]]+)\]", calc)
                        for field_ref in field_refs:
                            clean_field = field_ref.strip()
                            # Check if field is missing from our mapping (case-insensitive)
                            if not any(
                                existing.lower() == clean_field.lower()
                                for existing in field_table_mapping.keys()
                            ):
                                missing_fields.add(clean_field)

            # For missing fields, assign them to the most common table in existing mapping
            if missing_fields and field_table_mapping:
                from collections import Counter

                table_counts = Counter(field_table_mapping.values())
                most_common_table = table_counts.most_common(1)[0][0]

                for missing_field in missing_fields:
                    field_table_mapping[missing_field] = most_common_table
                    self.logger.debug(
                        f"Inferred missing field mapping: {missing_field} -> {most_common_table}"
                    )

            self.logger.debug(
                f"Enhanced field mapping to {len(field_table_mapping)} entries. "
                f"Added mappings for {len(missing_fields)} missing fields."
            )

        except Exception as e:
            self.logger.warning(
                f"Failed to process datasource-dependencies mappings: {e}"
            )

    def get_version(self) -> str:
        """Get version information."""
        return "1.0.0"


class MigrationError(Exception):
    """Raised when migration fails."""

    pass
