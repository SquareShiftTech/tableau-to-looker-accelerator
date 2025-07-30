"""
Enhanced Tableau XML Parser v2 - Metadata-First Approach

This parser prioritizes metadata-records for complete field coverage,
then enhances with column element details for calculated fields and captions.

Key improvements over v1:
- 100% field coverage using metadata-records as primary source
- Accurate SQL column mappings from remote-name/remote-alias
- Proper table associations from parent-name
- Automatic dimension/measure classification from aggregation
- Enhanced join generation with explicit field-table relationships
"""

import zipfile
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Union
from lxml import etree as ET
from lxml.etree import Element
import logging

logger = logging.getLogger(__name__)


class TableauParseError(Exception):
    """Exception raised for errors during Tableau file parsing."""

    pass


class TableauXMLParserV2:
    """Enhanced parser for Tableau workbook files (.twb and .twbx).

    Uses metadata-first approach for complete field coverage and accurate mappings.
    """

    def __init__(self, chunk_size: int = 65536):
        """Initialize parser with optional chunk size for streaming.

        Args:
            chunk_size: Size of chunks when reading large files
        """
        self.chunk_size = chunk_size
        self.logger = logging.getLogger(__name__)

    def parse_file(self, file_path: Union[str, Path]) -> Dict:
        """Parse a Tableau workbook file into structured data.

        Args:
            file_path: Path to .twb or .twbx file

        Returns:
            Dict containing parsed workbook data

        Raises:
            TableauParseError: If file cannot be parsed
            FileNotFoundError: If file doesn't exist
            ValueError: If file extension is not .twb or .twbx
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        ext = file_path.suffix.lower()
        if ext not in [".twb", ".twbx"]:
            raise ValueError(f"Unsupported file extension: {ext}")

        try:
            if ext == ".twb":
                return self._parse_twb_file(file_path)
            else:
                return self._parse_twbx_file(file_path)
        except Exception as e:
            raise TableauParseError(f"Failed to parse {file_path}: {str(e)}")

    def _parse_twb_file(self, file_path: Path) -> Element:
        """Parse a standalone .twb file.

        Args:
            file_path: Path to .twb file

        Returns:
            ElementTree root element
        """
        self.logger.info(f"Parsing TWB file: {file_path}")
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Log basic stats
        datasources = root.findall(".//datasource")
        self.logger.info(f"Found {len(datasources)} datasources")

        return root

    def _parse_twbx_file(self, file_path: Path) -> Element:
        """Parse a packaged .twbx file.

        Args:
            file_path: Path to .twbx file

        Returns:
            ElementTree root element from contained .twb file

        Raises:
            TableauParseError: If no .twb file found in package
        """
        with zipfile.ZipFile(file_path) as zf:
            # Find the .twb file
            twb_files = [f for f in zf.namelist() if f.endswith(".twb")]
            if not twb_files:
                raise TableauParseError("No .twb file found in .twbx package")

            # Use first .twb file found
            twb_file = twb_files[0]
            self.logger.info(f"Extracting {twb_file} from {file_path}")

            # Extract and parse the .twb file
            with zf.open(twb_file) as f:
                tree = ET.parse(f)
                return tree.getroot()

    def get_all_elements_enhanced(self, root: Element) -> List[Dict]:
        """Enhanced element extraction using metadata-first approach.

        This is the main improvement over v1 - prioritizes metadata-records
        for base field extraction, then enhances with column element details.

        Args:
            root: Root element of workbook

        Returns:
            List of enhanced element dictionaries with complete field coverage
        """
        elements = []

        # Process each datasource
        for datasource in root.findall(".//datasource"):
            self.logger.info(
                f"Processing datasource: {datasource.get('name', 'unnamed')}"
            )

            # Phase 1: Extract base fields from metadata-records (PRIMARY SOURCE)
            metadata_fields = self._extract_metadata_fields(datasource)
            self.logger.info(
                f"Extracted {len(metadata_fields)} fields from metadata-records"
            )

            # Phase 2: Extract calculated fields and enhancements from column elements
            column_enhancements = self._extract_column_enhancements(datasource)
            self.logger.info(f"Found {len(column_enhancements)} column enhancements")

            # Phase 3: Merge and enhance fields
            enhanced_fields = self._merge_field_data(
                metadata_fields, column_enhancements
            )
            self.logger.info(
                f"Created {len(enhanced_fields)} enhanced field definitions"
            )

            # Phase 4: Build table mapping for relationships
            table_mapping = self._build_enhanced_table_mapping(
                datasource, enhanced_fields
            )
            alias_mapping = self._build_alias_mapping(datasource)

            # Phase 5: Convert to element format for handlers
            elements.extend(
                self._convert_fields_to_elements(
                    enhanced_fields, table_mapping, alias_mapping
                )
            )

            # Phase 6: Add other element types (connections, relationships, parameters)
            elements.extend(self._extract_other_elements(datasource))

        self.logger.info(f"Total elements extracted: {len(elements)}")
        return elements

    def _extract_metadata_fields(self, datasource: Element) -> Dict[str, Dict]:
        """Extract base fields from metadata-records (PRIMARY SOURCE).

        This provides complete field coverage with accurate SQL mappings.

        Args:
            datasource: Datasource element to process

        Returns:
            Dict mapping field names to metadata field definitions
        """
        metadata_fields = {}

        for metadata in datasource.findall(".//metadata-record[@class='column']"):
            # Extract core metadata elements
            local_name_elem = metadata.find("local-name")
            remote_name_elem = metadata.find("remote-name")
            parent_name_elem = metadata.find("parent-name")
            remote_alias_elem = metadata.find("remote-alias")
            local_type_elem = metadata.find("local-type")
            aggregation_elem = metadata.find("aggregation")
            contains_null_elem = metadata.find("contains-null")

            # Skip if missing essential elements
            if not (local_name_elem is not None and remote_name_elem is not None):
                continue

            field_name = local_name_elem.text.strip("[]")

            # Determine field type from data type (BEST APPROACH)
            # This matches how Tableau actually classifies fields
            data_type = (
                local_type_elem.text if local_type_elem is not None else "string"
            )
            aggregation = (
                aggregation_elem.text if aggregation_elem is not None else "Count"
            )

            # Use data type for field classification (more reliable than aggregation):
            # - Numeric types (real, integer) → Measures (can be summed, averaged)
            # - Non-numeric types (string, date, boolean) → Dimensions (categorical/grouping)
            is_measure = data_type.lower() in [
                "real",
                "integer",
                "number",
                "float",
                "double",
            ]

            # Create enhanced field definition
            field_def = {
                # Core identification
                "field_name": field_name,
                "local_name": local_name_elem.text,
                "remote_name": remote_name_elem.text,
                # Table association (KEY IMPROVEMENT)
                "table_name": parent_name_elem.text.strip("[]")
                if parent_name_elem is not None
                else None,
                # SQL mapping (KEY IMPROVEMENT)
                "sql_column": remote_alias_elem.text
                if remote_alias_elem is not None
                else remote_name_elem.text,
                # Label from remote-alias (for LookML label field)
                "label": remote_alias_elem.text
                if remote_alias_elem is not None
                else remote_name_elem.text,
                # Type classification (KEY IMPROVEMENT)
                "field_type": "measure" if is_measure else "dimension",
                "role": "measure" if is_measure else "dimension",
                "datatype": data_type,
                "aggregation": aggregation,
                # Additional metadata
                "contains_null": contains_null_elem.text == "true"
                if contains_null_elem is not None
                else False,
                "source": "metadata_record",
                # Placeholders for column enhancements
                "caption": None,
                "calculation": None,
                "is_calculated": False,
                "drill_down": None,
                "number_format": None,
                "semantic_role": None,
                "folder": None,
                "description": None,
            }

            metadata_fields[field_name] = field_def

        self.logger.debug(
            f"Extracted {len(metadata_fields)} fields from metadata-records"
        )
        return metadata_fields

    def _extract_column_enhancements(self, datasource: Element) -> Dict[str, Dict]:
        """Extract enhancements and calculated fields from column elements.

        Args:
            datasource: Datasource element to process

        Returns:
            Dict mapping field names to column enhancement data
        """
        column_enhancements = {}

        for col in datasource.findall(".//column"):
            name = col.get("name", "").strip("[]")
            if not name:
                continue

            # Extract column-specific data
            enhancement = {
                "caption": col.get("caption"),
                "role": col.get("role"),
                "datatype": col.get("datatype"),
                "type": col.get("type"),
                "semantic_role": col.get("semantic-role"),
                "folder": col.get("folder"),
                "description": col.get("description"),
                "number_format": col.get("number-format"),
                "aggregation": col.get("aggregation"),
                "source": "column_element",
            }

            # Check for calculations (CALCULATED FIELDS)
            calc_element = col.find("calculation")
            if calc_element is not None:
                enhancement["calculation"] = calc_element.get("formula")
                enhancement["is_calculated"] = True
                enhancement["calculation_class"] = calc_element.get("class")
            else:
                enhancement["is_calculated"] = False

            # Check for drill-down settings
            drill_element = col.find("drill-down")
            if drill_element is not None:
                enhancement["drill_down"] = {
                    "fields": [f.get("name") for f in drill_element.findall("field")],
                    "default": drill_element.get("default", "false") == "true",
                }

            # Check for parameter domain settings
            param_domain = col.get("param-domain-type")
            if param_domain:
                enhancement["param_domain_type"] = param_domain
                enhancement["field_type"] = "parameter"

                # Get parameter values
                if param_domain == "list":
                    members = col.findall(".//member")
                    enhancement["values"] = [
                        m.get("value") for m in members if m.get("value")
                    ]
                elif param_domain == "range":
                    range_element = col.find("range")
                    if range_element is not None:
                        enhancement["range"] = {
                            "min": range_element.get("min"),
                            "max": range_element.get("max"),
                            "step": range_element.get("step", "1"),
                        }

                # Get default value
                default = col.find("default-value")
                if default is not None:
                    enhancement["default_value"] = default.get("value") or default.get(
                        "formula"
                    )

            column_enhancements[name] = enhancement

        self.logger.debug(f"Extracted {len(column_enhancements)} column enhancements")
        return column_enhancements

    def _merge_field_data(
        self, metadata_fields: Dict[str, Dict], column_enhancements: Dict[str, Dict]
    ) -> Dict[str, Dict]:
        """Merge metadata fields with column enhancements for complete field definitions.

        Args:
            metadata_fields: Base fields from metadata-records
            column_enhancements: Enhancements from column elements

        Returns:
            Dict of merged field definitions with complete data
        """
        enhanced_fields = {}

        # Start with metadata fields as the base (COMPLETE COVERAGE)
        for field_name, field_def in metadata_fields.items():
            enhanced_field = field_def.copy()

            # Enhance with column data if available
            if field_name in column_enhancements:
                enhancement = column_enhancements[field_name]

                # Apply enhancements, preferring column data for UI elements
                enhanced_field["caption"] = enhancement.get("caption") or field_def.get(
                    "caption"
                )
                enhanced_field["calculation"] = enhancement.get("calculation")
                enhanced_field["is_calculated"] = enhancement.get(
                    "is_calculated", False
                )
                enhanced_field["drill_down"] = enhancement.get("drill_down")
                enhanced_field["number_format"] = enhancement.get("number_format")
                enhanced_field["semantic_role"] = enhancement.get("semantic_role")
                enhanced_field["folder"] = enhancement.get("folder")
                enhanced_field["description"] = enhancement.get("description")

                # Override field type if it's a parameter
                if enhancement.get("param_domain_type"):
                    enhanced_field["field_type"] = "parameter"
                    enhanced_field["role"] = "parameter"
                    enhanced_field["param_domain_type"] = enhancement[
                        "param_domain_type"
                    ]
                    enhanced_field["values"] = enhancement.get("values", [])
                    enhanced_field["range"] = enhancement.get("range")
                    enhanced_field["default_value"] = enhancement.get("default_value")

                # Update source to indicate enhancement
                enhanced_field["source"] = "metadata_enhanced"

            enhanced_fields[field_name] = enhanced_field

        # Override field classification with XML role if available (AUTHORITATIVE)
        # This uses Tableau's actual classification from column elements
        for field_name, enhanced_field in enhanced_fields.items():
            if field_name in column_enhancements:
                xml_role = column_enhancements[field_name].get("role")
                if xml_role in ["dimension", "measure"]:
                    # Use Tableau's explicit classification
                    enhanced_field["field_type"] = xml_role
                    enhanced_field["role"] = xml_role

        # Add calculated fields that exist only in column elements (NO METADATA)
        for field_name, enhancement in column_enhancements.items():
            if field_name not in enhanced_fields and enhancement.get("is_calculated"):
                # This is a calculated field not in metadata
                calculated_field = {
                    "field_name": field_name,
                    "local_name": f"[{field_name}]",
                    "remote_name": None,  # Calculated fields don't have remote names
                    "table_name": None,  # Will be inferred from dependencies
                    "sql_column": None,  # Will be generated from calculation
                    "field_type": "calculated_field",
                    "role": enhancement.get("role", "measure"),
                    "datatype": enhancement.get("datatype", "real"),
                    "aggregation": enhancement.get("aggregation"),
                    "caption": enhancement.get("caption"),
                    "calculation": enhancement.get("calculation"),
                    "is_calculated": True,
                    "source": "column_calculated",
                    "contains_null": False,
                    "drill_down": enhancement.get("drill_down"),
                    "number_format": enhancement.get("number_format"),
                    "semantic_role": enhancement.get("semantic_role"),
                    "folder": enhancement.get("folder"),
                    "description": enhancement.get("description"),
                }
                enhanced_fields[field_name] = calculated_field

        self.logger.info(f"Merged {len(enhanced_fields)} enhanced field definitions")
        return enhanced_fields

    def _build_enhanced_table_mapping(
        self, datasource: Element, enhanced_fields: Dict[str, Dict]
    ) -> Dict[str, str]:
        """Build enhanced field-to-table mapping from metadata-based fields.

        This replaces the complex inference logic from v1 with direct metadata extraction.

        Args:
            datasource: Datasource element
            enhanced_fields: Enhanced field definitions

        Returns:
            Dict mapping field names to table names
        """
        table_mapping = {}

        # Use direct table associations from metadata (PRIMARY)
        for field_name, field_def in enhanced_fields.items():
            table_name = field_def.get("table_name")
            if table_name:
                table_mapping[field_name] = table_name

        self.logger.debug(
            f"Built enhanced table mapping with {len(table_mapping)} entries"
        )
        return table_mapping

    def _build_alias_mapping(self, datasource: Element) -> Dict[str, str]:
        """Build mapping from table aliases to actual table names.

        Reused from v1 parser - this logic is still needed for join processing.

        Args:
            datasource: Datasource element containing relationships with table aliases

        Returns:
            Dict mapping alias names to actual table names
        """
        alias_mapping = {}

        # Get all actual table names from the datasource
        actual_tables = set()
        for relation in datasource.findall(".//relation[@type='table']"):
            name = relation.get("name")
            if name:
                actual_tables.add(name)

        # Look for table aliases in object-graph
        for rel in datasource.findall(
            ".//object-graph//object/properties/relation[@type='table']"
        ):
            name = rel.get("name")
            if name:
                actual_tables.add(name)

        # Process relationships to find aliases
        rel_data = self.extract_relationships(datasource)
        for relationship in rel_data.get("relationships", []):
            table_aliases = relationship.get("table_aliases", {})
            for alias, actual_table in table_aliases.items():
                # Clean the actual table name (remove brackets and schema)
                clean_actual = (
                    actual_table.split(".")[-1].strip("[]")
                    if "." in actual_table
                    else actual_table.strip("[]")
                )

                # If this points to an actual table, map the alias
                if clean_actual in actual_tables or alias in actual_tables:
                    # Use the actual table name (the one in our tables array)
                    if alias in actual_tables:
                        alias_mapping[alias] = alias  # Direct mapping
                    else:
                        # Find the corresponding actual table name
                        for table_name in actual_tables:
                            if table_name in actual_table or clean_actual == table_name:
                                alias_mapping[alias] = table_name
                                break

        return alias_mapping

    def _convert_fields_to_elements(
        self,
        enhanced_fields: Dict[str, Dict],
        table_mapping: Dict[str, str],
        alias_mapping: Dict[str, str],
    ) -> List[Dict]:
        """Convert enhanced field definitions to element format for handlers.

        Args:
            enhanced_fields: Enhanced field definitions
            table_mapping: Field to table mapping
            alias_mapping: Table alias mapping

        Returns:
            List of element dictionaries for handler processing
        """
        elements = []

        for field_name, field_def in enhanced_fields.items():
            # Resolve table alias
            table_name = field_def.get("table_name")
            resolved_table = self._resolve_table_alias(table_name, alias_mapping)
            field_def["table_name"] = resolved_table

            # Create element based on field type
            field_type = field_def.get("field_type")

            if field_type in ["measure", "dimension"]:
                # Convert to handler format - use REMOTE-NAME for clean field names
                element_data = {
                    "name": field_def["remote_name"],  # ✅ Use clean DB column name
                    "raw_name": field_def["local_name"],  # Keep original for reference
                    "role": field_def["role"],
                    "datatype": field_def["datatype"],
                    "table_name": resolved_table,
                    "sql_column": field_def["sql_column"],  # KEY ENHANCEMENT
                    "aggregation": field_def.get("aggregation"),
                    "caption": field_def.get("caption"),
                    "calculation": field_def.get("calculation"),
                    "number_format": field_def.get("number_format"),
                    "drill_down": field_def.get("drill_down"),
                    "semantic_role": field_def.get("semantic_role"),
                    "folder": field_def.get("folder"),
                    "description": field_def.get("description"),
                    "label": field_def.get("label")  # Preserve remote-alias label
                    or field_def.get("caption")
                    or field_def["local_name"],  # User-friendly label
                }

                elements.append({"type": field_type, "data": element_data})

            elif field_type == "calculated_field":
                # Calculated fields get special handling
                element_data = {
                    "name": field_def["local_name"],
                    "raw_name": field_def["local_name"],
                    "role": field_def["role"],
                    "datatype": field_def["datatype"],
                    "table_name": resolved_table,  # Will be inferred by handler
                    "calculation": field_def["calculation"],
                    "caption": field_def.get("caption"),
                    "aggregation": field_def.get("aggregation"),
                    "number_format": field_def.get("number_format"),
                    "label": field_def.get("label") or field_def.get("caption"),
                }

                elements.append({"type": "calculated_field", "data": element_data})

            elif field_type == "parameter":
                # Parameters get special handling
                element_data = {
                    "name": field_def["local_name"],
                    "raw_name": field_def["local_name"],
                    "role": "parameter",
                    "datatype": field_def["datatype"],
                    "param_domain_type": field_def.get("param_domain_type"),
                    "values": field_def.get("values", []),
                    "range": field_def.get("range"),
                    "default_value": field_def.get("default_value"),
                    "caption": field_def.get("caption"),
                    "description": field_def.get("description"),
                    "label": field_def.get("label") or field_def.get("caption"),
                }

                elements.append({"type": "parameter", "data": element_data})

        return elements

    def _extract_other_elements(self, datasource: Element) -> List[Dict]:
        """Extract non-field elements (connections, relationships) using existing v1 methods.

        Args:
            datasource: Datasource element

        Returns:
            List of other element types
        """
        elements = []

        # Add connections
        for conn in datasource.findall(".//connection"):
            elements.append(
                {"type": "connection", "data": self.extract_connection(conn)}
            )

        # Add relationships
        rel_data = self.extract_relationships(datasource)
        if rel_data["tables"] or rel_data["relationships"]:
            elements.append({"type": "relationships", "data": rel_data})

        return elements

    def _resolve_table_alias(
        self, table_name: str, alias_mapping: Dict[str, str]
    ) -> str:
        """Resolve a table alias to the actual table name.

        Args:
            table_name: Table name (possibly an alias)
            alias_mapping: Mapping from aliases to actual table names

        Returns:
            Actual table name
        """
        if not table_name:
            return table_name

        # Return the resolved table name or the original if not found
        return alias_mapping.get(table_name, table_name)

    # ============================================================================
    # REUSED METHODS FROM V1 PARSER (for compatibility)
    # These methods handle relationships, connections, and other non-field elements
    # ============================================================================

    def get_element_by_id(self, root: Element, element_id: str) -> Optional[Element]:
        """Find element by its ID attribute."""
        return root.find(f".//*[@id='{element_id}']")

    def get_elements_by_name(self, root: Element, tag_name: str) -> Iterator[Element]:
        """Find all elements with given tag name."""
        return root.iter(tag_name)

    def element_to_dict(self, element: Element) -> Dict:
        """Convert XML element to dictionary."""
        result = dict(element.attrib)
        if element.text and element.text.strip():
            result["text"] = element.text.strip()
        return result

    def extract_connection(self, element: Element) -> Dict:
        """Extract connection data from connection element."""
        # Get basic attributes
        data = {
            "name": element.get("name", ""),
            "server": element.get("server", ""),
            "username": element.get("username", ""),
            "dbname": element.get("dbname", ""),
            "type": element.get("type", ""),
            "connection_type": element.get("connection-type", ""),
            "class": element.get("class", ""),
            "authentication": element.get("authentication", ""),
            "port": element.get("port"),
            "schema": element.get("schema"),
            "connection_string": element.get("connection-string"),
            "workgroup": element.get("workgroup"),
            "query_band": element.get("query-band"),
            "metadata": {},
        }

        # Get connection metadata
        metadata = element.find("metadata")
        if metadata is not None:
            for item in metadata.findall("metadata-record"):
                key = item.get("key")
                value = item.get("value")
                if key and value:
                    data["metadata"][key] = value

        return data

    def extract_table_info(self, element: Element) -> Optional[Dict]:
        """Extract table information from a relation element."""
        if element.get("type") != "table":
            return None

        connection = element.get("connection")
        name = element.get("name")
        table = element.get("table")

        if not (connection and table):
            return None

        return {"connection": connection, "name": name, "table": table}

    def extract_physical_join(self, element: Element) -> Optional[Dict]:
        """Extract physical join information from a relation element."""
        if element.get("type") != "join":
            return None

        join_type = element.get("join", "inner")
        clause = element.find('clause[@type="join"]')

        if clause is None:
            return None

        expression = clause.find("expression")
        if expression is None:
            return None

        # Extract expressions
        expr_data = {"operator": expression.get("op", "="), "expressions": []}

        for expr in expression.findall("expression"):
            if expr.text:
                expr_data["expressions"].append(expr.text)
            elif expr.get("op"):
                expr_data["expressions"].append(expr.get("op"))

        # Extract tables and their aliases
        tables = []
        table_aliases = {}

        for rel in element.findall("relation"):
            if rel.get("type") == "table":
                table_info = self.extract_table_info(rel)
                if table_info:
                    tables.append(table_info)
                    # Map alias to actual table
                    alias = table_info.get("name")
                    actual_table = table_info.get("table")
                    if alias and actual_table:
                        table_aliases[alias] = actual_table
            elif rel.get("type") == "join":
                # For nested joins, we need to recursively extract tables
                nested_join = self.extract_physical_join(rel)
                if nested_join:
                    tables.extend(nested_join.get("tables", []))
                    table_aliases.update(nested_join.get("table_aliases", {}))

        return {
            "join_type": join_type,
            "expression": expr_data,
            "tables": tables,
            "table_aliases": table_aliases,
        }

    def extract_logical_join(self, element: Element) -> Optional[Dict]:
        """Extract logical join information from a relationship element."""
        if element.tag != "relationship":
            return None

        # Required elements
        expression = element.find("expression")
        first_endpoint = element.find("first-end-point")
        second_endpoint = element.find("second-end-point")

        if None in (expression, first_endpoint, second_endpoint):
            return None

        # Extract endpoints
        def get_endpoint_info(endpoint):
            object_id = endpoint.get("object-id")
            if not object_id:
                return None

            # Find object by ID - navigate to object-graph
            relationship = endpoint.getparent()
            if relationship is None:
                return None
            relationships = relationship.getparent()
            if relationships is None:
                return None
            object_graph = relationships.getparent()
            if object_graph is None:
                return None

            objects = object_graph.find("objects")
            if objects is None:
                return None

            obj = objects.find(f".//object[@id='{object_id}']")
            if obj is None:
                return None

            props = obj.find("properties")
            if props is None:
                return None

            rel = props.find("relation")
            if rel is None:
                return None

            # For logical relationships, some relations might be joins without table info
            # Use caption as fallback for table name
            table_name = rel.get("table") or obj.get("caption")

            return {
                "object_id": object_id,
                "caption": obj.get("caption"),
                "connection": rel.get("connection"),
                "name": rel.get("name") or obj.get("caption"),
                "table": table_name,
            }

        first_info = get_endpoint_info(first_endpoint)
        second_info = get_endpoint_info(second_endpoint)

        if not (first_info and second_info):
            return None

        # Extract expression
        expr_data = {"operator": expression.get("op", "="), "expressions": []}

        for expr in expression.findall("expression"):
            if expr.text:
                expr_data["expressions"].append(expr.text)
            elif expr.get("op"):
                expr_data["expressions"].append(expr.get("op"))

        return {
            "expression": expr_data,
            "first_endpoint": first_info,
            "second_endpoint": second_info,
        }

    def extract_relationships(self, datasource: Element) -> Dict:
        """Extract all relationships from a datasource element."""
        tables = []
        relationships = []

        # Extract all tables
        for search_path in [".//relation", ".//object-graph//relation"]:
            for relation in datasource.findall(search_path):
                table_info = self.extract_table_info(relation)
                if table_info and table_info not in tables:
                    tables.append(table_info)

        # Extract physical joins
        for join_rel in datasource.findall(".//relation[@type='join']"):
            join_info = self.extract_physical_join(join_rel)
            if join_info:
                relationships.append({"relationship_type": "physical", **join_info})

        # Extract logical joins
        for object_graph in datasource.findall(".//object-graph"):
            rels = object_graph.find("relationships")
            if rels is not None:
                for rel in rels.findall("relationship"):
                    join_info = self.extract_logical_join(rel)
                    if join_info:
                        relationships.append(
                            {"relationship_type": "logical", **join_info}
                        )

        return {"tables": tables, "relationships": relationships}

    # ============================================================================
    # BACKWARD COMPATIBILITY METHOD
    # This allows v2 to be a drop-in replacement for v1
    # ============================================================================

    def get_all_elements(self, root: Element) -> List[Dict]:
        """Backward compatibility method - delegates to enhanced version.

        This allows v2 parser to be a drop-in replacement for v1.

        Args:
            root: Root element of workbook

        Returns:
            List of element dictionaries (enhanced)
        """
        return self.get_all_elements_enhanced(root)
