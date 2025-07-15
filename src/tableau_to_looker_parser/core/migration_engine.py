import json
import logging
from pathlib import Path
from typing import Any, Dict

from tableau_to_looker_parser.core.xml_parser import TableauXMLParser
from tableau_to_looker_parser.core.plugin_registry import PluginRegistry
from tableau_to_looker_parser.handlers.base_handler import BaseHandler
from tableau_to_looker_parser.handlers.relationship_handler import RelationshipHandler
from tableau_to_looker_parser.handlers.connection_handler import ConnectionHandler
from tableau_to_looker_parser.handlers.dimension_handler import DimensionHandler
from tableau_to_looker_parser.handlers.measure_handler import MeasureHandler
from tableau_to_looker_parser.handlers.parameter_handler import ParameterHandler


class MigrationEngine:
    """Orchestrates the entire Tableau to LookML conversion process.

    Manages the conversion pipeline:
    1. XML parsing
    2. Data extraction using handlers
    3. JSON output generation
    4. LookML generation (future)
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.plugin_registry = PluginRegistry()

        # Register default handlers
        self.register_handler(RelationshipHandler(), priority=1)
        self.register_handler(ConnectionHandler(), priority=2)
        self.register_handler(DimensionHandler(), priority=3)
        self.register_handler(MeasureHandler(), priority=4)
        self.register_handler(ParameterHandler(), priority=5)

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
            # Parse workbook
            parser = TableauXMLParser()
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
            }

            # Process with handlers using clean architecture
            self.logger.info("Starting workbook processing")

            # Get all elements from parser
            elements = parser.get_all_elements(root)
            self.logger.info(f"Found {len(elements)} elements to process")

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
                        json_data = handler.convert_to_json(element_data)

                        # Route to appropriate result category
                        if element["type"] == "measure":
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

    def get_version(self) -> str:
        """Get version information."""
        return "1.0.0"


class MigrationError(Exception):
    """Raised when migration fails."""

    pass
