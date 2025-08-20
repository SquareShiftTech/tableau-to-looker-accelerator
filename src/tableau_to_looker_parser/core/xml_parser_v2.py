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
from typing import Dict, Iterator, List, Optional, Union, Any
from lxml import etree as ET
from lxml.etree import Element
import logging
from .tableau_style_extractor import TableauStyleExtractor

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
        self.style_extractor = TableauStyleExtractor()

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
                # User-friendly label (unified logic)
                "label": self._get_user_friendly_label(
                    caption=None,
                    local_name=local_name_elem.text
                    if local_name_elem is not None
                    else None,
                    remote_alias=remote_alias_elem.text
                    if remote_alias_elem is not None
                    else None,
                ),
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
                "is_internal": self._is_internal_field(col),
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

    def _is_internal_field(self, col_element: Element) -> bool:
        """Detect internal Tableau fields generically.

        Args:
            col_element: Column XML element

        Returns:
            bool: True if field is internal/system-generated
        """
        internal_indicators = [
            col_element.get("is-adhoc-cluster") == "true",
            col_element.get("parent-model") is not None,
            col_element.get("auto-hidden") == "true",
            col_element.get("hidden") == "true"
            and col_element.get("system-generated") == "true",
        ]
        return any(internal_indicators)

    def _get_user_friendly_label(
        self,
        caption: Optional[str],
        local_name: Optional[str],
        remote_alias: Optional[str],
    ) -> str:
        """Get user-friendly label for fields in consistent priority order.

        Args:
            caption: User-set field caption from Tableau
            local_name: Tableau local name like "[Order ID]"
            remote_alias: Database column name like "Order_ID"

        Returns:
            str: User-friendly label for LookML
        """
        # Priority 1: Caption (user-set name in Tableau)
        if caption and caption.strip():
            return caption.strip()

        # Priority 2: Local name cleaned up (remove brackets, convert underscores to spaces)
        if local_name and local_name.strip():
            cleaned = local_name.strip("[]").replace("_", " ")
            return cleaned

        # Priority 3: Remote alias as fallback
        if remote_alias and remote_alias.strip():
            # Convert database column name to readable format
            return remote_alias.replace("_", " ")

        return "Unknown Field"

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
                    "label": self._get_user_friendly_label(
                        caption=field_def.get("caption"),
                        local_name=field_def["local_name"],
                        remote_alias=field_def.get("label"),
                    ),
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
                    "label": self._get_user_friendly_label(
                        caption=field_def.get("caption"),
                        local_name=field_def["local_name"],
                        remote_alias=field_def.get("label"),
                    ),
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
                    "label": self._get_user_friendly_label(
                        caption=field_def.get("caption"),
                        local_name=field_def["local_name"],
                        remote_alias=field_def.get("label"),
                    ),
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

    # ============================================================================
    # PHASE 3: WORKSHEET AND DASHBOARD EXTRACTION METHODS
    # ============================================================================

    def extract_worksheets(self, root: Element) -> List[Dict]:
        """Extract all worksheet elements from Tableau XML.

        Args:
            root: Root element of the workbook

        Returns:
            List of worksheet dictionaries with field usage and visualization config
        """
        worksheets = []

        for worksheet in root.findall(".//worksheet"):
            worksheet_name = worksheet.get("name")

            if worksheet_name == "byTypeS apple":
                print(f"Worksheet {worksheet_name} has data: {worksheet}")

            if not worksheet_name:
                continue

            try:
                worksheet_data = {
                    "name": worksheet_name,
                    "clean_name": self._clean_name(worksheet_name),
                    "title": self._extract_worksheet_title(worksheet),
                    "datasource_id": self._extract_worksheet_datasource_id(worksheet),
                    "fields": self._extract_worksheet_fields(worksheet),
                    "visualization": self._extract_visualization_config(worksheet),
                    "filters": self._extract_worksheet_filters(worksheet),
                    "sorts": self._extract_worksheet_sorts(worksheet),
                    "actions": self._extract_worksheet_actions(worksheet),
                }

                # NEW: Extract styling data using separate module (non-breaking)
                try:
                    styling_data = self.style_extractor.extract_worksheet_styling(
                        worksheet, worksheet_name
                    )
                    if styling_data and any(
                        styling_data.values()
                    ):  # Only add if we found styling
                        worksheet_data["styling"] = styling_data
                        self.logger.debug(
                            f"Extracted styling for worksheet: {worksheet_name}"
                        )
                except Exception as style_error:
                    # Don't break worksheet processing if styling extraction fails
                    self.logger.warning(
                        f"Failed to extract styling for worksheet '{worksheet_name}': {style_error}"
                    )

                worksheets.append(worksheet_data)

            except Exception as e:
                self.logger.warning(
                    f"Failed to parse worksheet '{worksheet_name}': {e}"
                )
                continue

        self.logger.info(f"Extracted {len(worksheets)} worksheets")
        return worksheets

    def extract_color_palettes(self, root: Element) -> Dict[str, List[str]]:
        """Extract all color palettes from Tableau XML.

        Args:
            root: Root element of the workbook

        Returns:
            Dictionary mapping palette names to color lists
        """
        palettes = {}

        # Extract custom color palettes
        for palette in root.findall(".//color-palette"):
            palette_name = palette.get("name")
            palette_type = palette.get("type", "regular")

            if palette_name:
                colors = []
                for color in palette.findall("color"):
                    if color.text:
                        colors.append(color.text.strip())

                if colors:
                    palettes[palette_name] = {
                        "colors": colors,
                        "type": palette_type,
                        "custom": palette.get("custom", "false") == "true",
                    }

        # Extract default Tableau palette if no custom ones found
        if not palettes:
            palettes["default"] = {
                "colors": [
                    "#4E79A7",
                    "#F28E2B",
                    "#E15759",
                    "#76B7B2",
                    "#59A14F",
                    "#EDC948",
                    "#B07AA1",
                    "#FF9DA7",
                    "#BAB0AC",
                ],
                "type": "regular",
                "custom": False,
            }

        self.logger.info(f"Extracted {len(palettes)} color palettes")
        return palettes

    def extract_field_encodings(self, root: Element) -> Dict[str, Dict]:
        """Extract field encoding information (color, size, etc.) from worksheets.

        Args:
            root: Root element of the workbook

        Returns:
            Dictionary mapping worksheet names to their encoding configurations
        """
        worksheet_encodings = {}

        for worksheet in root.findall(".//worksheet"):
            worksheet_name = worksheet.get("name")
            if not worksheet_name:
                continue

            encodings = {
                "color_fields": [],
                "size_fields": [],
                "detail_fields": [],
                "text_fields": [],
                "color_palettes": [],
            }

            # Extract from panes
            for pane in worksheet.findall(".//pane"):
                pane_encodings = pane.find("encodings")
                if pane_encodings is not None:
                    for encoding in pane_encodings:
                        encoding_type = encoding.tag
                        field = encoding.get("column", "")
                        palette = encoding.get("palette", "")

                        if encoding_type == "color":
                            encodings["color_fields"].append(
                                {
                                    "field": field,
                                    "palette": palette,
                                    "type": encoding.get("type", ""),
                                }
                            )
                            if palette:
                                encodings["color_palettes"].append(palette)
                        elif encoding_type == "size":
                            encodings["size_fields"].append(field)
                        elif encoding_type == "detail":
                            encodings["detail_fields"].append(field)
                        elif encoding_type == "text":
                            encodings["text_fields"].append(field)

            worksheet_encodings[worksheet_name] = encodings

        self.logger.info(
            f"Extracted encodings for {len(worksheet_encodings)} worksheets"
        )
        return worksheet_encodings

    def extract_dashboards(self, root: Element) -> List[Dict]:
        """Extract all dashboard elements from Tableau XML.

        Args:
            root: Root element of the workbook

        Returns:
            List of dashboard dictionaries with zones and layout information
        """
        dashboards = []

        for dashboard in root.findall(".//dashboard"):
            dashboard_name = dashboard.get("name")
            if not dashboard_name:
                continue

            try:
                dashboard_data = {
                    "name": dashboard_name,
                    "clean_name": self._clean_name(dashboard_name),
                    "title": dashboard_name.replace("_", " ").title(),
                    "canvas_size": self._extract_dashboard_size(dashboard),
                    "elements": self._extract_dashboard_elements(dashboard),
                    "layout_type": self._determine_layout_type(dashboard),
                    "global_filters": self._extract_dashboard_filters(dashboard),
                    "responsive_config": self._extract_responsive_config(dashboard),
                }
                dashboards.append(dashboard_data)

            except Exception as e:
                self.logger.warning(
                    f"Failed to parse dashboard '{dashboard_name}': {e}"
                )
                continue

        self.logger.info(f"Extracted {len(dashboards)} dashboards")
        return dashboards

    # ============================================================================
    # WORKSHEET PARSING HELPER METHODS
    # ============================================================================

    def _extract_worksheet_datasource_id(self, worksheet: Element) -> Optional[str]:
        """Extract the primary datasource ID for a worksheet."""
        datasource_elem = worksheet.find(".//datasource")
        if datasource_elem is not None:
            return datasource_elem.get("name") or datasource_elem.get("caption")
        return None

    def _extract_worksheet_fields(self, worksheet: Element) -> List[Dict]:
        """Extract field usage from worksheet datasource-dependencies."""
        fields = []

        # Find all datasource-dependencies elements
        all_dependencies = worksheet.findall(".//datasource-dependencies")
        if not all_dependencies:
            return fields

        # Process each datasource-dependencies element
        for dependencies in all_dependencies:
            # Extract column instances (actual field usage)
            for column_instance in dependencies.findall("column-instance"):
                field_data = self._parse_column_instance(column_instance, worksheet)
                if field_data:
                    fields.append(field_data)

        return fields

    def _lookup_field_caption(
        self, worksheet: Element, column_ref: str
    ) -> Optional[str]:
        """Look up field caption from top-level datasource column definitions."""
        # Get the root workbook element to access top-level datasources
        root = worksheet
        while root.getparent() is not None:
            root = root.getparent()

        # Look in all top-level datasources for the column definition
        for datasource in root.findall(".//datasources/datasource"):
            for column in datasource.findall(".//column"):
                if column.get("name") == column_ref:
                    caption = column.get("caption")
                    # Return caption if it exists and is not 'None'
                    if caption and caption != "None":
                        return caption

        return None

    def _lookup_column_definition(
        self, worksheet: Element, column_ref: str
    ) -> Optional[Dict]:
        """Look up column definition from top-level datasource for authoritative field info."""
        # Get the root workbook element to access top-level datasources
        root = worksheet
        while root.getparent() is not None:
            root = root.getparent()

        # Look in all top-level datasources for the column definition
        for datasource in root.findall(".//datasources/datasource"):
            for column in datasource.findall(".//column"):
                if column.get("name") == column_ref:
                    return {
                        "type": column.get("type"),
                        "role": column.get("role"),
                        "caption": column.get("caption"),
                        "datatype": column.get("datatype"),
                    }

        return None

    def _parse_column_instance(
        self, column_instance: Element, worksheet: Element
    ) -> Optional[Dict]:
        """Parse a column-instance element into field reference data."""
        column_ref = column_instance.get("column", "")
        instance_name = column_instance.get("name", "")
        derivation = column_instance.get("derivation", "None")
        # pivot = column_instance.get("pivot", "key")

        if not column_ref or not instance_name:
            return None

        # Clean field name
        field_name = column_ref.strip("[]")
        clean_name = self._clean_name(field_name)

        # Look up column definition for authoritative field_type and role
        column_def = self._lookup_column_definition(worksheet, column_ref)

        if column_def is not None:
            # Use authoritative values from column definition
            field_type = column_def.get("type", "nominal")  # Tableau field type
            role = column_def.get("role", "dimension")  # Tableau role
            caption = column_def.get("caption")  # Caption from column
            datatype = column_def.get("datatype", "string")  # Data type
        else:
            # Fallback to column-instance values if no column definition found
            field_type = column_instance.get("type", "nominal")
            role = "measure" if field_type == "quantitative" else "dimension"
            caption = self._lookup_field_caption(worksheet, column_ref)
            datatype = self._infer_datatype_from_type(field_type)

        # Determine shelf placement and encoding type
        shelf_info = self._determine_field_shelf_and_encoding(worksheet, instance_name)

        return {
            "name": clean_name,
            "original_name": column_ref,
            "tableau_instance": instance_name,
            "datatype": datatype,
            "role": role,
            "aggregation": derivation if derivation != "None" else None,
            "shelf": shelf_info["shelf"],
            "encodings": shelf_info["encodings"],  # List of all encodings
            "derivation": derivation,
            "caption": caption,
        }

    def _determine_field_shelf_and_encoding(
        self, worksheet: Element, instance_name: str
    ) -> Dict:
        """Determine both shelf placement and encoding types for a field instance."""
        shelf = "detail"  # Default shelf
        encodings_list = []  # List of all encodings

        # Check rows shelf
        rows_elem = worksheet.find(".//rows")
        if rows_elem is not None and instance_name in (rows_elem.text or ""):
            shelf = "rows"

        # Check columns shelf
        cols_elem = worksheet.find(".//cols")
        if cols_elem is not None and instance_name in (cols_elem.text or ""):
            shelf = "columns"

        # Check all encodings (color, size, text, etc.) - CHECK ALL PANES
        panes = worksheet.findall(".//pane")

        for pane_idx, pane in enumerate(panes):
            # Check direct encoding attributes
            encodings = pane.find("encodings")
            if encodings is not None:
                for child in encodings:
                    encoding_column = child.get("column", "")

                    # Check for exact match or suffix match (handle federated prefix)
                    if (
                        encoding_column == instance_name
                        or encoding_column.endswith(f"].{instance_name}")
                        or encoding_column.endswith(instance_name)
                    ):
                        if child.tag not in encodings_list:  # Avoid duplicates
                            encodings_list.append(child.tag)

        # Set shelf based on primary encoding if found
        if "color" in encodings_list:
            shelf = "color"
        elif "size" in encodings_list:
            shelf = "size"
        elif "text" in encodings_list:
            shelf = "text"

        return {"shelf": shelf, "encodings": encodings_list}

    def _determine_field_shelf(self, worksheet: Element, instance_name: str) -> str:
        """Legacy method - determine which shelf a field instance is placed on."""
        shelf_info = self._determine_field_shelf_and_encoding(worksheet, instance_name)
        return shelf_info["shelf"]

    def _extract_visualization_config(self, worksheet: Element) -> Dict:
        """Extract visualization configuration from worksheet panes."""
        pane = worksheet.find(".//pane")
        if pane is None:
            return {
                "chart_type": "automatic",
                "x_axis": [],
                "y_axis": [],
                "color": None,
                "size": None,
                "detail": [],
                "tooltip": [],
                "raw_config": {"chart_type": "automatic"},
            }

        # Extract mark type (chart type) - RAW DATA ONLY
        mark = pane.find("mark")
        chart_type = (
            mark.get("class", "automatic").lower() if mark is not None else "automatic"
        )

        # Extract encodings - RAW DATA ONLY
        encodings_info = self._extract_pane_encodings(pane)

        # NEW: Extract pane-level styling
        pane_styling = self._extract_pane_styling(pane)

        if pane_styling:
            self.logger.debug(f"Extracted pane styling: {list(pane_styling.keys())}")
            for element_type, rules in pane_styling.items():
                self.logger.debug(f"  {element_type}: {len(rules)} styling rules")
        else:
            self.logger.debug("No pane styling found")

        # Extract field mappings
        viz_config = {
            "chart_type": chart_type,  # Raw chart type from XML
            "x_axis": self._extract_shelf_fields(worksheet, "cols"),
            "y_axis": self._extract_shelf_fields(worksheet, "rows"),
            "color": None,
            "size": None,
            "detail": [],
            "tooltip": [],
            "is_dual_axis": self._has_dual_axis(worksheet),
            "show_labels": self._extract_show_labels(pane),
            "show_totals": self._extract_show_totals(worksheet),
            "raw_config": {
                "chart_type": chart_type,
                "mark_class": chart_type,
                "encodings": encodings_info,  # Raw encoding data for handler
                "pane_styling": pane_styling,  # NEW: Pane-specific styling
            },
        }

        # Extract encodings for backwards compatibility
        encodings = pane.find("encodings")
        if encodings is not None:
            for encoding in encodings:
                encoding_type = encoding.tag
                column = encoding.get("column", "")

                if encoding_type == "color":
                    viz_config["color"] = column
                elif encoding_type == "size":
                    viz_config["size"] = column
                elif encoding_type == "detail":
                    viz_config["detail"].append(column)

        return viz_config

    def _extract_pane_encodings(self, pane: Element) -> Dict:
        """Extract raw encoding information from pane."""
        encodings_info = {
            "text_columns": [],
            "color_columns": [],
            "size_columns": [],
            "detail_columns": [],
        }

        encodings = pane.find("encodings")
        if encodings is not None:
            for encoding in encodings:
                encoding_type = encoding.tag
                column = encoding.get("column", "")

                if encoding_type == "text":
                    encodings_info["text_columns"].append(column)
                elif encoding_type == "color":
                    encodings_info["color_columns"].append(column)
                elif encoding_type == "size":
                    encodings_info["size_columns"].append(column)
                elif encoding_type == "detail":
                    encodings_info["detail_columns"].append(column)

        return encodings_info

    def _extract_shelf_fields(self, worksheet: Element, shelf_name: str) -> List[str]:
        """Extract field names from a specific shelf (rows/cols)."""
        shelf_elem = worksheet.find(f".//{shelf_name}")
        if shelf_elem is None or not shelf_elem.text:
            return []

        # Parse field references from shelf text
        fields = []
        shelf_text = shelf_elem.text

        # Extract field instance names (format: [datasource].[field_instance])
        import re

        field_pattern = r"\[([^\]]+)\]\.\[([^\]]+)\]"
        matches = re.findall(field_pattern, shelf_text)

        for datasource, field_instance in matches:
            fields.append(field_instance)

        return fields

    def _extract_worksheet_sorts(self, worksheet: Element) -> List[Dict]:
        """Extract sorting configuration from worksheet."""
        sorts = []

        for sort in worksheet.findall(".//shelf-sort-v2"):
            sort_config = {
                "field": sort.get("dimension-to-sort", ""),
                "direction": sort.get("direction", "ASC"),
                "sort_by_field": sort.get("measure-to-sort-by"),
                "is_innermost": sort.get("is-on-innermost-dimension") == "true",
            }
            sorts.append(sort_config)

        return sorts

    def _extract_worksheet_filters(self, worksheet: Element) -> List[Dict]:
        """Extract filter configuration from worksheet.

        Parses BOTH:
        1. <filter> elements (actual filter definitions - THE REAL FILTERS)
        2. <card type='filter'> elements (UI filter cards)

        Extensible design to handle new filter types as they're discovered.
        """
        filters = []

        try:
            # Method 1: Extract ACTUAL filter definitions from <filter> elements
            filter_definitions = worksheet.findall(".//filter")
            for filter_elem in filter_definitions:
                filter_data = self._parse_filter_definition(filter_elem)
                if filter_data:
                    filters.append(filter_data)

            # Method 2: Extract UI filter cards from <card type='filter'> elements
            filter_cards = worksheet.findall(".//card[@type='filter']")

            # Method 3: Check associated worksheet window for filter cards
            worksheet_name = worksheet.get("name")
            if worksheet_name:
                root = worksheet
                while root.getparent() is not None:
                    root = root.getparent()

                window = root.find(
                    f".//window[@class='worksheet'][@name='{worksheet_name}']"
                )
                if window is not None:
                    window_filter_cards = window.findall(".//card[@type='filter']")
                    filter_cards.extend(window_filter_cards)

            # Parse all found filter cards
            for card in filter_cards:
                filter_data = self._parse_filter_card(card)
                if filter_data:
                    pass
                    # filters.append(filter_data)

        except Exception as e:
            self.logger.warning(f"Failed to extract worksheet filters: {e}")
            # Return empty list to avoid breaking existing functionality

        return filters

    # ============================================================================
    # FILTER PARSING HELPER METHODS - FULLY GENERIC & EXTENSIBLE
    # ============================================================================

    def _parse_filter_card(self, card: Element) -> Optional[Dict]:
        """Parse worksheet filter card - completely generic.

        No hardcoded values - extracts all attributes for extensibility.
        """
        try:
            param = card.get("param")
            if not param:
                return None

            # Parse field reference generically
            field_info = self._parse_filter_field_reference(param)
            if not field_info:
                return None

            # Generic extraction of ALL card attributes
            filter_config = {
                "field_name": field_info["field_name"],
                "field_reference": param,
                "datasource_id": field_info.get("datasource_id"),
                "filter_type": "worksheet_card",
                # Extract ALL attributes generically - no hardcoding
                **{attr: value for attr, value in card.attrib.items()},
                "field_info": field_info,
                "position_context": self._extract_card_position_context(card),
            }

            return filter_config

        except Exception as e:
            self.logger.warning(f"Failed to parse filter card: {e}")
            return None

    def _parse_filter_zone(self, zone: Element) -> Optional[Dict]:
        """Parse dashboard filter zone - completely generic.

        No hardcoded values - extracts all attributes for extensibility.
        """
        try:
            param = zone.get("param")
            if not param:
                return None

            # Parse field reference generically
            field_info = self._parse_filter_field_reference(param)
            if not field_info:
                return None

            # Generic extraction of ALL zone attributes
            filter_config = {
                "field_name": field_info["field_name"],
                "field_reference": param,
                "datasource_id": field_info.get("datasource_id"),
                "filter_type": "dashboard_zone",
                # Extract ALL attributes generically - no hardcoding
                **{attr: value for attr, value in zone.attrib.items()},
                "field_info": field_info,
                "position": self._extract_zone_position(zone)
                if hasattr(self, "_extract_zone_position")
                else {},
            }

            return filter_config

        except Exception as e:
            self.logger.warning(f"Failed to parse filter zone: {e}")
            return None

    def _parse_filter_field_reference(self, param: str) -> Optional[Dict]:
        """Parse filter field reference - completely generic.

        Handles any field reference pattern without hardcoding.
        """
        if not param:
            return None

        try:
            # Generic parsing of complex field references
            field_info = {
                "original_param": param,
                "clean_param": param.strip("[]"),
            }

            # Handle federated references: [datasource].[field]
            if "].[" in param:
                parts = param.split("].[", 1)
                field_info["datasource_id"] = parts[0].strip("[")
                field_reference = parts[1].strip("]")
            else:
                field_info["datasource_id"] = ""
                field_reference = param.strip("[]")

            # Parse field components generically: type:name:qualifier
            components = field_reference.split(":")
            field_info["components"] = components
            field_info["field_name"] = (
                components[1] if len(components) > 1 else field_reference
            )
            field_info["field_type"] = components[0] if len(components) > 0 else ""
            field_info["field_qualifier"] = components[2] if len(components) > 2 else ""

            # Store all components for extensibility
            if len(components) > 3:
                field_info["additional_components"] = components[3:]

            return field_info

        except Exception as e:
            self.logger.warning(
                f"Failed to parse filter field reference '{param}': {e}"
            )
            return None

    def _extract_card_position_context(self, card: Element) -> Dict:
        """Extract positioning context for filter card - generic."""
        context = {}

        try:
            # Get parent strip info generically
            parent = card.getparent()
            if parent is not None:
                context["parent_tag"] = parent.tag
                context["parent_attributes"] = dict(parent.attrib)

                # Get grandparent context
                grandparent = parent.getparent()
                if grandparent is not None:
                    context["grandparent_tag"] = grandparent.tag
                    context["grandparent_attributes"] = dict(grandparent.attrib)

        except Exception:
            pass

        return context

    def _extract_worksheet_actions(self, worksheet: Element) -> List[Dict]:
        """Extract action configuration from worksheet."""
        # This is a placeholder - actions are complex
        # Would need to parse action elements, URL actions, filter actions, etc.
        return []

    # ============================================================================
    # DASHBOARD PARSING HELPER METHODS
    # ============================================================================

    def _extract_dashboard_size(self, dashboard: Element) -> Dict[str, int]:
        """Extract dashboard canvas size configuration."""
        size_elem = dashboard.find("size")
        if size_elem is None:
            return {"width": 1000, "height": 800}

        return {
            "width": int(size_elem.get("maxwidth", "1000")),
            "height": int(size_elem.get("maxheight", "800")),
            "min_width": int(size_elem.get("minwidth", "800")),
            "min_height": int(size_elem.get("minheight", "600")),
        }

    def _extract_dashboard_elements(self, dashboard: Element) -> List[Dict]:
        """Extract all dashboard elements with positioning."""
        elements = []

        # Extract zones with content ONLY from main dashboard zones (not device layouts)
        # This avoids duplicates from mobile/tablet responsive layouts
        main_zones = dashboard.find("zones")
        if main_zones is not None:
            for zone in main_zones.findall(".//zone[@name]"):
                element = self._parse_dashboard_zone(zone)
                if element:
                    elements.append(element)

        return elements

    def _parse_dashboard_zone(self, zone: Element) -> Optional[Dict]:
        """Parse a dashboard zone into an element dictionary."""
        zone_id = zone.get("id")
        zone_name = zone.get("name")

        if not zone_id or not zone_name:
            return None

        # Extract position
        position = self._extract_zone_position(zone)

        # Extract styling
        style = self._extract_zone_style(zone)

        # Determine element type and content
        element_type, content = self._determine_zone_content(zone)

        element = {
            "element_id": zone_id,
            "element_type": element_type,
            "position": position,
            "style": style,
            "is_interactive": True,
            "interactions": [],
        }

        # Add type-specific content
        if element_type == "worksheet":
            element["worksheet_name"] = zone_name
        elif element_type == "filter":
            element["filter_config"] = content
        elif element_type == "parameter":
            element["parameter_config"] = content
        elif element_type == "text":
            element["text_content"] = zone_name

        return element

    def _extract_zone_position(self, zone: Element) -> Dict[str, float]:
        """Extract and normalize zone position."""
        x = int(zone.get("x", "0"))
        y = int(zone.get("y", "0"))
        width = int(zone.get("w", "100000"))
        height = int(zone.get("h", "100000"))

        # Normalize to 0-1 coordinates (Tableau uses 100000 as full scale)
        return {
            "x": x / 100000,
            "y": y / 100000,
            "width": width / 100000,
            "height": height / 100000,
            "z_index": 0,
        }

    def _extract_zone_style(self, zone: Element) -> Dict[str, Any]:
        """Extract zone styling information."""
        style = {
            "background_color": None,
            "border_color": None,
            "border_width": 0,
            "border_style": "none",
            "margin": 4,
            "padding": 0,
        }

        zone_style = zone.find("zone-style")
        if zone_style is not None:
            for format_elem in zone_style.findall("format"):
                attr = format_elem.get("attr")
                value = format_elem.get("value")

                if attr == "border-color":
                    style["border_color"] = value
                elif attr == "border-width":
                    style["border_width"] = int(value) if value.isdigit() else 0
                elif attr == "border-style":
                    style["border_style"] = value
                elif attr == "margin":
                    style["margin"] = int(value) if value.isdigit() else 4

        return style

    def _determine_zone_content(self, zone: Element) -> tuple[str, Optional[Dict]]:
        """Determine zone content type and extract relevant data."""
        zone_type = zone.get("type-v2", "")
        param = zone.get("param", "")

        if zone_type == "color":
            return "filter", {
                "filter_type": "color",
                "field": param,
                "filter_values": [],
            }
        elif param:
            return "parameter", {"parameter_name": param, "parameter_type": "unknown"}
        else:
            # Default to worksheet
            return "worksheet", None

    def _extract_responsive_config(self, dashboard: Element) -> Dict[str, Any]:
        """Extract responsive/device layout configuration."""
        responsive = {}

        device_layouts = dashboard.find("devicelayouts")
        if device_layouts is not None:
            for device_layout in device_layouts.findall("devicelayout"):
                device_name = device_layout.get("name", "").lower()
                if device_name:
                    responsive[device_name] = {
                        "auto_generated": device_layout.get("auto-generated") == "true"
                    }

        return responsive

    # ============================================================================
    # UTILITY HELPER METHODS
    # ============================================================================

    def _clean_name(self, name: str) -> str:
        """Convert name to LookML-safe format."""
        import re

        # Remove brackets, convert to lowercase, replace special chars with underscore
        clean = name.strip("[]").lower()
        clean = re.sub(r"[^a-z0-9]+", "_", clean)
        clean = re.sub(r"_+", "_", clean)  # Remove duplicate underscores
        return clean.strip("_")

    def _infer_datatype_from_type(self, field_type: str) -> str:
        """Infer datatype from Tableau field type."""
        type_mapping = {
            "quantitative": "real",
            "nominal": "string",
            "ordinal": "string",
            "temporal": "date",
        }
        return type_mapping.get(field_type, "string")

    def _has_dual_axis(self, worksheet: Element) -> bool:
        """Check if worksheet uses dual axis."""
        # Look for dual axis indicators in the worksheet XML
        panes = worksheet.findall(".//pane")
        return len(panes) > 1

    def _extract_show_labels(self, pane: Element) -> bool:
        """Extract whether data labels are shown."""
        style_rule = pane.find('.//style-rule[@element="mark"]')
        if style_rule is not None:
            format_elem = style_rule.find('format[@attr="mark-labels-show"]')
            if format_elem is not None:
                return format_elem.get("value") == "true"
        return False

    def _extract_show_totals(self, worksheet: Element) -> bool:
        """Extract whether totals are shown."""
        # Look for totals configuration in worksheet
        totals = worksheet.find(".//totals")
        return totals is not None

    def _extract_worksheet_title(self, worksheet: Element) -> str:
        """Extract worksheet title from layout options."""
        # Look for title in layout-options/title/formatted-text/run
        title_elem = worksheet.find(".//layout-options/title/formatted-text/run")
        if title_elem is not None and title_elem.text:
            return title_elem.text.strip()

        # Fallback to worksheet name if no title found
        worksheet_name = worksheet.get("name", "Untitled")
        return worksheet_name.replace("_", " ").title()

    def _determine_layout_type(self, dashboard: Element) -> str:
        """
        Determine dashboard layout type from Tableau layout information.

        Tableau Layout Types → LookML Mapping:
        - layout-basic (absolute positioning) → free_form
        - layout-flow horizontal → grid (horizontal flow)
        - layout-flow vertical → newspaper (vertical stacking)
        - Mixed flows → newspaper (complex grid)
        - Floating elements → free_form
        """
        # Look for the main layout container
        main_zones = dashboard.find("zones")
        if main_zones is None:
            return "free_form"  # Fallback if no zones found

        # Analyze layout structure from Tableau's layout system
        layout_analysis = self._analyze_tableau_layout_structure(main_zones)

        # Map Tableau layout patterns to LookML layout types
        if layout_analysis["has_complex_flows"]:
            # Mixed horizontal/vertical flows with distribution strategies
            return "newspaper"
        elif layout_analysis["primary_flow"] == "horizontal":
            # Primarily horizontal flow layout
            return "grid"
        elif layout_analysis["primary_flow"] == "vertical":
            # Primarily vertical flow layout (dashboard style)
            return "newspaper"
        elif layout_analysis["has_distributed_elements"]:
            # Elements with distribute-evenly strategy
            return "newspaper"
        elif layout_analysis["has_fixed_elements"]:
            # Elements with fixed positioning
            return "grid"
        elif layout_analysis["element_density"] == "high":
            # Many elements suggest structured layout
            return "newspaper"
        else:
            # Default to free-form for simple/unclear layouts
            return "free_form"

    def _analyze_tableau_layout_structure(self, main_zones: Element) -> Dict[str, Any]:
        """Analyze Tableau's layout structure to determine optimal LookML layout."""
        analysis = {
            "has_complex_flows": False,
            "primary_flow": None,
            "has_distributed_elements": False,
            "has_fixed_elements": False,
            "element_density": "low",
            "flow_patterns": [],
        }

        # Find all zones with layout information
        all_zones = main_zones.findall(".//zone")
        named_zones = [z for z in all_zones if z.get("name")]

        # Analyze flow layouts
        flow_zones = [z for z in all_zones if z.get("type-v2") == "layout-flow"]
        horizontal_flows = [z for z in flow_zones if z.get("param") == "horz"]
        vertical_flows = [z for z in flow_zones if z.get("param") == "vert"]

        # Analyze distribution strategies
        distributed_zones = [
            z for z in all_zones if z.get("layout-strategy-id") == "distribute-evenly"
        ]
        fixed_zones = [z for z in all_zones if z.get("is-fixed") == "true"]

        # Determine primary flow direction
        if horizontal_flows and vertical_flows:
            analysis["has_complex_flows"] = True
            analysis["primary_flow"] = "mixed"
        elif len(horizontal_flows) > len(vertical_flows):
            analysis["primary_flow"] = "horizontal"
        elif len(vertical_flows) > len(horizontal_flows):
            analysis["primary_flow"] = "vertical"

        # Check for distribution strategies
        if distributed_zones:
            analysis["has_distributed_elements"] = True

        # Check for fixed positioning
        if fixed_zones:
            analysis["has_fixed_elements"] = True

        # Determine element density
        if len(named_zones) > 8:
            analysis["element_density"] = "high"
        elif len(named_zones) > 4:
            analysis["element_density"] = "medium"
        else:
            analysis["element_density"] = "low"

        # Record flow patterns for debugging
        analysis["flow_patterns"] = [
            {"type": "horizontal", "count": len(horizontal_flows)},
            {"type": "vertical", "count": len(vertical_flows)},
            {"type": "distributed", "count": len(distributed_zones)},
            {"type": "fixed", "count": len(fixed_zones)},
        ]

        return analysis

    def _extract_dashboard_filters(self, dashboard: Element) -> List[Dict]:
        """Extract dashboard-level filters.

        Parses <zone type-v2='filter'> elements from dashboard zones.
        Extensible design to handle new filter types as they're discovered.
        """
        filters = []

        try:
            # Find all filter zones in dashboard
            filter_zones = dashboard.findall(".//zone[@type-v2='filter']")

            for zone in filter_zones:
                filter_data = self._parse_filter_zone(zone)
                if filter_data:
                    filters.append(filter_data)

        except Exception as e:
            self.logger.warning(f"Failed to extract dashboard filters: {e}")
            # Return empty list to avoid breaking existing functionality

        return filters

    # ============================================================================
    # FILTER PARSING HELPER METHODS - FULLY GENERIC & EXTENSIBLE
    # ============================================================================

    def _parse_filter_card(self, card: Element) -> Optional[Dict]:
        """Parse worksheet filter card - completely generic.

        No hardcoded values - extracts all attributes for extensibility.
        """
        try:
            param = card.get("param")
            if not param:
                return None

            # Parse field reference generically
            field_info = self._parse_filter_field_reference(param)
            if not field_info:
                return None

            # Generic extraction of ALL card attributes
            filter_config = {
                "field_name": field_info["field_name"],
                "field_reference": param,
                "datasource_id": field_info.get("datasource_id"),
                "filter_type": "worksheet_card",
                # Extract ALL attributes generically - no hardcoding
                **{attr: value for attr, value in card.attrib.items()},
                "field_info": field_info,
                "position_context": self._extract_card_position_context(card),
            }

            return filter_config

        except Exception as e:
            self.logger.warning(f"Failed to parse filter card: {e}")
            return None

    def _parse_filter_zone(self, zone: Element) -> Optional[Dict]:
        """Parse dashboard filter zone - completely generic.

        No hardcoded values - extracts all attributes for extensibility.
        """
        try:
            param = zone.get("param")
            if not param:
                return None

            # Parse field reference generically
            field_info = self._parse_filter_field_reference(param)
            if not field_info:
                return None

            # Generic extraction of ALL zone attributes
            filter_config = {
                "field_name": field_info["field_name"],
                "field_reference": param,
                "datasource_id": field_info.get("datasource_id"),
                "filter_type": "dashboard_zone",
                # Extract ALL attributes generically - no hardcoding
                **{attr: value for attr, value in zone.attrib.items()},
                "field_info": field_info,
                "position": self._extract_zone_position(zone)
                if hasattr(self, "_extract_zone_position")
                else {},
            }

            return filter_config

        except Exception as e:
            self.logger.warning(f"Failed to parse filter zone: {e}")
            return None

    def _parse_filter_field_reference(self, param: str) -> Optional[Dict]:
        """Parse filter field reference - completely generic.

        Handles any field reference pattern without hardcoding.
        """
        if not param:
            return None

        try:
            # Generic parsing of complex field references
            field_info = {
                "original_param": param,
                "clean_param": param.strip("[]"),
            }

            # Handle federated references: [datasource].[field]
            if "].[" in param:
                parts = param.split("].[", 1)
                field_info["datasource_id"] = parts[0].strip("[")
                field_reference = parts[1].strip("]")
            else:
                field_info["datasource_id"] = ""
                field_reference = param.strip("[]")

            # Parse field components generically: type:name:qualifier
            components = field_reference.split(":")
            field_info["components"] = components
            field_info["field_name"] = (
                components[1] if len(components) > 1 else field_reference
            )
            field_info["field_type"] = components[0] if len(components) > 0 else ""
            field_info["field_qualifier"] = components[2] if len(components) > 2 else ""

            # Store all components for extensibility
            if len(components) > 3:
                field_info["additional_components"] = components[3:]

            return field_info

        except Exception as e:
            self.logger.warning(
                f"Failed to parse filter field reference '{param}': {e}"
            )
            return None

    def _extract_card_position_context(self, card: Element) -> Dict:
        """Extract positioning context for filter card - generic."""
        context = {}

        try:
            # Get parent strip info generically
            parent = card.getparent()
            if parent is not None:
                context["parent_tag"] = parent.tag
                context["parent_attributes"] = dict(parent.attrib)

                # Get grandparent context
                grandparent = parent.getparent()
                if grandparent is not None:
                    context["grandparent_tag"] = grandparent.tag
                    context["grandparent_attributes"] = dict(grandparent.attrib)

        except Exception:
            pass

        return context

    def _parse_filter_definition(self, filter_elem: Element) -> Optional[Dict]:
        """Parse <filter class='categorical'> elements - completely generic.

        Extracts actual filter logic from Tableau filter definitions.
        """
        try:
            # Extract basic filter attributes
            filter_class = filter_elem.get("class", "")
            column = filter_elem.get("column", "")
            filter_group = filter_elem.get("filter-group", "")

            # Parse field reference from column attribute
            field_info = self._parse_filter_field_reference(column)
            if not field_info:
                return None

            # Generic extraction of ALL filter attributes
            filter_config = {
                "field_name": field_info.get("field_name", ""),
                "field_reference": column,
                "datasource_id": field_info.get("datasource_id", ""),
                "filter_type": "filter_definition",
                "filter_class": filter_class,
                "filter_group": filter_group,
                "field_info": field_info,
                "groupfilter_logic": [],
            }

            # Extract all attributes generically
            for attr, value in filter_elem.attrib.items():
                if attr not in filter_config:
                    filter_config[attr] = value

            # Parse groupfilter logic
            groupfilters = filter_elem.findall(".//groupfilter")
            for groupfilter in groupfilters:
                groupfilter_data = self._parse_groupfilter_logic(groupfilter)
                if groupfilter_data:
                    filter_config["groupfilter_logic"].append(groupfilter_data)

            return filter_config

        except Exception as e:
            self.logger.warning(f"Failed to parse filter definition: {e}")
            return None

    def _parse_groupfilter_logic(self, groupfilter: Element) -> Optional[Dict]:
        """Parse groupfilter logic within filter definitions - completely generic."""
        try:
            # Extract function type and attributes
            function = groupfilter.get("function", "")
            level = groupfilter.get("level", "")
            member = groupfilter.get("member", "")

            # Generic extraction of ALL groupfilter attributes
            groupfilter_data = {"function": function, "level": level, "member": member}

            # Extract all attributes generically
            for attr, value in groupfilter.attrib.items():
                if attr not in groupfilter_data:
                    groupfilter_data[attr] = value

            # Handle nested groupfilters (for crossjoin, union, etc.)
            nested_groupfilters = groupfilter.findall("groupfilter")
            if nested_groupfilters:
                groupfilter_data["nested_filters"] = []
                for nested in nested_groupfilters:
                    nested_data = self._parse_groupfilter_logic(nested)
                    if nested_data:
                        groupfilter_data["nested_filters"].append(nested_data)

            return groupfilter_data

        except Exception as e:
            self.logger.warning(f"Failed to parse groupfilter logic: {e}")
            return None

    def _extract_pane_styling(self, pane: Element) -> Dict[str, Any]:
        """
        Extract pane-level styling information from <style> elements inside the pane.

        Args:
            pane: Pane XML element

        Returns:
            Dict containing pane-specific styling rules
        """
        try:
            pane_styling = {}

            # Find all <style> elements within this pane
            style_elements = pane.findall(".//style")

            if style_elements:
                self.logger.debug(f"Found {len(style_elements)} style elements in pane")

            for style in style_elements:
                # Find style-rule elements within this style
                style_rules = style.findall(".//style-rule")

                for rule in style_rules:
                    element_type = rule.get("element", "unknown")

                    # Extract format attributes
                    formats = rule.findall(".//format")
                    rule_formats = {}

                    for fmt in formats:
                        attr = fmt.get("attr", "")
                        value = fmt.get("value", "")
                        if attr and value:
                            rule_formats[attr] = value

                    # Store the styling rule
                    if element_type not in pane_styling:
                        pane_styling[element_type] = []

                    pane_styling[element_type].append(
                        {
                            "formats": rule_formats
                            # Removed raw_rule to avoid JSON serialization issues
                        }
                    )

                    self.logger.debug(
                        f"Extracted {element_type} styling rule with {len(rule_formats)} formats: {list(rule_formats.keys())}"
                    )

            if pane_styling:
                self.logger.debug(
                    f"Extracted pane styling for elements: {list(pane_styling.keys())}"
                )

            return pane_styling

        except Exception as e:
            self.logger.warning(f"Failed to extract pane styling: {str(e)}")
            return {}
