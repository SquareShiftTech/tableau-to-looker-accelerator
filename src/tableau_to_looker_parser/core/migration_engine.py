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
from tableau_to_looker_parser.handlers.worksheet_handler import WorksheetHandler
from tableau_to_looker_parser.handlers.dashboard_handler import DashboardHandler


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

        # Register default handlers (Phase 1-2)
        self.register_handler(RelationshipHandler(), priority=1)
        self.register_handler(ConnectionHandler(), priority=2)
        self.register_handler(DimensionHandler(), priority=3)
        self.register_handler(MeasureHandler(), priority=4)
        self.register_handler(ParameterHandler(), priority=5)
        self.register_handler(
            CalculatedFieldHandler(), priority=6
        )  # After regular fields

        # Register Phase 3 handlers (worksheets and dashboards)
        self.register_handler(WorksheetHandler(), priority=7)
        self.register_handler(DashboardHandler(), priority=8)

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
                "color_palettes": {},  # Will be populated from Tableau XML
                "field_encodings": {},  # Will be populated from Tableau XML
                "calculated_fields": [],
                # Phase 3: Worksheet and Dashboard data
                "worksheets": [],
                "dashboards": [],
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
                            # Handle two-step pattern from measure handler
                            if json_data.get("two_step_pattern"):
                                # Add hidden dimension to dimensions
                                result["dimensions"].append(json_data["dimension"])
                                # Add measure to measures
                                result["measures"].append(json_data["measure"])
                            else:
                                # Standard single measure
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

            # Phase 3: Process worksheets and dashboards (only with v2 parser)
            if self.use_v2_parser:
                self.logger.info("Processing Phase 3: Worksheets and Dashboards")
                self._process_worksheets_and_dashboards(parser, root, result)

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

        When multiple datasources have the same field name, creates unique keys
        by prefixing with datasource/table name to avoid conflicts.

        Args:
            elements: List of parsed elements from XMLParser

        Returns:
            Dict mapping field names to their table names
        """
        field_table_mapping = {}
        field_occurrences = {}  # Track how many times each field name appears

        # First pass: count field name occurrences across all datasources
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
                    if field_name in field_occurrences:
                        field_occurrences[field_name].add(table_name)
                    else:
                        field_occurrences[field_name] = {table_name}

        # Second pass: build mapping with conflict resolution
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
                    # If field name appears in multiple tables, create qualified keys
                    if len(field_occurrences.get(field_name, set())) > 1:
                        # Create qualified key: table_name.field_name
                        qualified_key = f"{table_name}.{field_name}"
                        field_table_mapping[qualified_key] = table_name
                        # Also keep the unqualified key pointing to the first occurrence
                        # for backward compatibility
                        if field_name not in field_table_mapping:
                            field_table_mapping[field_name] = table_name
                    else:
                        # Unique field name, use it directly
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

    def _process_worksheets_and_dashboards(self, parser, root, result: Dict) -> None:
        """
        Process worksheets and dashboards using Phase 3 handlers.

        This is the integration layer that:
        1. Extracts raw worksheet and dashboard data using XMLParser
        2. Processes them through dedicated handlers
        3. Links worksheets to dashboard elements
        4. Adds complete integrated data to result
        """
        try:
            # Step 1: Extract raw data using XMLParser
            self.logger.info("Extracting raw worksheets and dashboards")
            raw_worksheets = parser.extract_worksheets(root)
            raw_dashboards = parser.extract_dashboards(root)

            # Extract styling information from Tableau XML
            self.logger.info("Extracting color palettes and field encodings")
            color_palettes = parser.extract_color_palettes(root)
            field_encodings = parser.extract_field_encodings(root)

            # Add styling information to result
            result["color_palettes"] = color_palettes
            result["field_encodings"] = field_encodings

            self.logger.info(
                f"Found {len(raw_worksheets)} worksheets, {len(raw_dashboards)} dashboards, "
                f"{len(color_palettes)} color palettes, and encodings for {len(field_encodings)} worksheets"
            )

            # Step 2: Process worksheets through WorksheetHandler
            worksheet_handler = WorksheetHandler()
            processed_worksheets = {}  # name -> worksheet mapping for linking

            for raw_worksheet in raw_worksheets:
                if worksheet_handler.can_handle(raw_worksheet) > 0:
                    processed = worksheet_handler.convert_to_json(raw_worksheet)
                    processed_worksheets[processed["name"]] = processed
                    result["worksheets"].append(processed)

                    # NEW: Route identified worksheet measures through MeasureHandler
                    identified_measures = processed.get("identified_measures", [])
                    for measure_data in identified_measures:
                        # Route through existing handler infrastructure
                        measure_handler = MeasureHandler()
                        if measure_handler.can_handle(measure_data) > 0:
                            json_data = measure_handler.convert_to_json(measure_data)

                            # Handle two-step pattern routing (same as base measures)
                            if json_data.get("two_step_pattern"):
                                result["dimensions"].append(json_data["dimension"])
                                result["measures"].append(json_data["measure"])
                            else:
                                result["measures"].append(json_data)

                    if identified_measures:
                        self.logger.info(
                            f"Routed {len(identified_measures)} worksheet measures through MeasureHandler"
                        )

                    self.logger.info(
                        f"Processed worksheet: {processed['name']} "
                        f"({processed['visualization']['chart_type']}, "
                        f"{len(processed['fields'])} fields)"
                    )

            # Step 3: Process dashboards through DashboardHandler
            dashboard_handler = DashboardHandler()

            for raw_dashboard in raw_dashboards:
                if dashboard_handler.can_handle(raw_dashboard) > 0:
                    processed = dashboard_handler.convert_to_json(raw_dashboard)

                    # Step 4: INTEGRATION - Link worksheets to dashboard elements
                    self._link_worksheets_to_dashboard(processed, processed_worksheets)

                    result["dashboards"].append(processed)

                    linked_count = sum(
                        1
                        for elem in processed["elements"]
                        if elem["element_type"] == "worksheet"
                        and elem["worksheet"] is not None
                    )

                    self.logger.info(
                        f"Processed dashboard: {processed['name']} "
                        f"({len(processed['elements'])} elements, "
                        f"{linked_count} worksheets linked)"
                    )

            self.logger.info("Phase 3 processing completed successfully")

        except Exception as e:
            self.logger.error(f"Phase 3 processing failed: {str(e)}", exc_info=True)
            # Don't raise - allow migration to continue with Phase 1-2 data

    def _link_worksheets_to_dashboard(
        self, dashboard: Dict, worksheets: Dict[str, Dict]
    ) -> None:
        """
        Link worksheet objects to dashboard elements.

        This is the core integration logic that makes dashboard elements self-contained
        by embedding the full worksheet data instead of just references.
        """
        for element in dashboard["elements"]:
            if element["element_type"] == "worksheet":
                worksheet_name = element["custom_content"].get("worksheet_name")

                if worksheet_name and worksheet_name in worksheets:
                    # INTEGRATION: Embed full worksheet data in dashboard element
                    element["worksheet"] = worksheets[worksheet_name]

                    # Clean up the reference since we now have the full data
                    element["custom_content"] = {}

                    self.logger.debug(
                        f"Linked worksheet '{worksheet_name}' to dashboard element {element['element_id']}"
                    )
                else:
                    self.logger.warning(
                        f"Worksheet '{worksheet_name}' not found for dashboard element {element['element_id']}"
                    )

    def get_version(self) -> str:
        """Get version information."""
        return "1.0.0"


class MigrationError(Exception):
    """Raised when migration fails."""

    pass
