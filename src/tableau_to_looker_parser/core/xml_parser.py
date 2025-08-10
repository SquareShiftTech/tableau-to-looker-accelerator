import zipfile
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Union
from lxml import etree as ET
from lxml.etree import Element


class TableauParseError(Exception):
    """Exception raised for errors during Tableau file parsing."""

    pass


class TableauXMLParser:
    """Parser for Tableau workbook files (.twb and .twbx).

    Single responsibility: Parse XML into structured data.
    - No business logic
    - No data transformation
    - Only XML operations
    """

    def __init__(self, chunk_size: int = 65536):
        """Initialize parser with optional chunk size for streaming.

        Args:
            chunk_size: Size of chunks when reading large files
        """
        self.chunk_size = chunk_size

    def parse_file(self, file_path: Union[str, Path]) -> Dict:
        """Parse a Tableau workbook file into structured data.

        Args:
            file_path: Path to .twb or .twbx file

        Returns:
            Dict containing:
            {
                "metadata": {
                    "source_file": str,
                    "workbook_version": str
                },
                "elements": {
                    "columns": [
                        {
                            "name": str,
                            "role": str,
                            "datatype": str,
                            "semantic_role": str,
                            ...
                        }
                    ],
                    "relationships": [
                        {
                            "type": str,
                            "join_type": str,
                            "tables": List[str],
                            "expression": Dict
                        }
                    ],
                    "connections": [...]
                }
            }

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
        # Debug parsing
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Debug object graphs
        for graph in root.findall(".//object-graph"):
            # Print object count
            objects = graph.find("objects")
            if objects is not None:
                for obj in objects:
                    print(
                        f"  - {obj.tag} id={obj.get('id')} caption={obj.get('caption')}"
                    )

            # Print relationship count
            rels = graph.find("relationships")
            if rels is not None:
                for rel in rels:
                    print(f"  - {rel.tag}")
                    expr = rel.find("expression")
                    if expr is not None:
                        for e in expr.findall("expression"):
                            if e.text:
                                print(f"      - Text: {e.text}")
                            if e.get("op"):
                                print(f"      - Op: {e.get('op')}")

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

            # Extract and parse the .twb file
            with zf.open(twb_file) as f:
                tree = ET.parse(f)
                return tree.getroot()

    def _stream_parse(self, context: Iterator) -> Element:
        """Stream parse XML using iterparse.

        Args:
            context: iterparse context

        Returns:
            Root element

        Note:
            Keeps references to all elements since we need to traverse them
        """
        # Get the root element
        _, root = next(context)

        # Process all other elements
        for _, elem in context:
            pass  # Just iterate through to build the tree

        return root

    def get_element_by_id(self, root: Element, element_id: str) -> Optional[Element]:
        """Find element by its ID attribute.

        Args:
            root: Root element to search in
            element_id: ID to find

        Returns:
            Matching element or None if not found
        """
        return root.find(f".//*[@id='{element_id}']")

    def get_elements_by_name(self, root: Element, tag_name: str) -> Iterator[Element]:
        """Find all elements with given tag name.

        Args:
            root: Root element to search in
            tag_name: Tag name to find

        Returns:
            Iterator of matching elements
        """
        return root.iter(tag_name)

    def element_to_dict(self, element: Element) -> Dict:
        """Convert XML element to dictionary.

        Args:
            element: XML element

        Returns:
            Dict with element's attributes and text
        """
        result = dict(element.attrib)
        if element.text and element.text.strip():
            result["text"] = element.text.strip()
        return result

    def extract_measure(self, element: Element) -> Dict:
        """Extract measure data from column element.

        Args:
            element: Column element with role='measure'

        Returns:
            Dict containing raw measure data:
            {
                "name": str,
                "raw_name": str,
                "role": "measure",
                "datatype": str,
                "aggregation": str,
                "number_format": Optional[str],
                "calculation": Optional[str],
                "drill_down": Optional[Dict],
                "label": Optional[str]
            }
        """
        # Get basic attributes
        name = element.get("name", "")

        data = {
            "name": name,
            "raw_name": name,
            "role": "measure",
            "datatype": element.get("datatype", "real"),
            "aggregation": element.get("aggregation", "sum"),
            "number_format": element.get("number-format"),
            "label": element.get("caption"),
        }

        # Get calculation if present
        calc_element = element.find("calculation")
        if calc_element is not None:
            data["calculation"] = calc_element.get("formula")

        # Get drill-down settings if present
        drill_element = element.find("drill-down")
        if drill_element is not None:
            data["drill_down"] = {
                "fields": [f.get("name") for f in drill_element.findall("field")],
                "default": drill_element.get("default", "false") == "true",
            }

        return data

    def extract_dimension(self, element: Element) -> Dict:
        """Extract dimension data from column element.

        Args:
            element: Column element with role='dimension'

        Returns:
            Dict containing raw dimension data:
            {
                "name": str,
                "raw_name": str,
                "role": "dimension",
                "datatype": str,
                "type": str,
                "semantic_role": Optional[str],
                "calculation": Optional[str],
                "default_aggregate": Optional[str],
                "folder": Optional[str],
                "label": Optional[str],
                "description": Optional[str]
            }
        """
        # Get basic attributes
        name = element.get("name", "")

        data = {
            "name": name,
            "raw_name": name,
            "role": "dimension",
            "datatype": element.get("datatype", "string"),
            "type": element.get("type", "nominal"),
            "semantic_role": element.get("semantic-role"),
            "default_aggregate": element.get("default-aggregate"),
            "folder": element.get("folder"),
            "label": element.get("caption"),
            "description": element.get("description"),
        }

        # Get calculation if present
        calc_element = element.find("calculation")
        if calc_element is not None:
            data["calculation"] = calc_element.get("formula")

        return data

    def extract_parameter(self, element: Element) -> Dict:
        """Extract parameter data from column element.

        Args:
            element: Column element with param-domain-type

        Returns:
            Dict containing raw parameter data:
            {
                "name": str,
                "raw_name": str,
                "role": "parameter",
                "datatype": str,
                "param_domain_type": str,
                "values": List[str],
                "range": Optional[Dict],
                "default_value": Optional[str],
                "label": Optional[str],
                "description": Optional[str]
            }
        """
        # Get basic attributes
        name = element.get("name", "")
        domain_type = element.get("param-domain-type")

        data = {
            "name": name,
            "raw_name": name,
            "role": "parameter",
            "datatype": element.get("datatype", "string"),
            "param_domain_type": domain_type,
            "label": element.get("caption"),
            "description": element.get("description"),
            "values": [],
        }

        # Get allowed values
        if domain_type == "list":
            members = element.findall(".//member")
            data["values"] = [m.get("value") for m in members if m.get("value")]

        # Get range settings
        elif domain_type == "range":
            range_element = element.find("range")
            if range_element is not None:
                data["range"] = {
                    "min": range_element.get("min"),
                    "max": range_element.get("max"),
                    "step": range_element.get("step", "1"),
                }

        # Get default value
        default = element.find("default-value")
        if default is not None:
            data["default_value"] = default.get("value") or default.get("formula")

        return data

    def extract_connection(self, element: Element) -> Dict:
        """Extract connection data from connection element.

        Args:
            element: Connection element

        Returns:
            Dict containing raw connection data:
            {
                "name": str,
                "server": str,
                "username": str,
                "dbname": str,
                "type": str,
                "connection_type": str,
                "class": str,
                "authentication": str,
                "port": Optional[str],
                "schema": Optional[str],
                "connection_string": Optional[str],
                "workgroup": Optional[str],
                "query_band": Optional[str],
                "metadata": Dict
            }
        """
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

    def get_datasources(self, root: Element) -> List[Dict]:
        """Get all datasource elements from workbook.

        Args:
            root: Root element of workbook

        Returns:
            List of extracted datasource dicts
        """
        datasources = []
        for ds in root.findall(".//datasource"):
            # Extract basic datasource info
            data = {
                "name": ds.get("name", ""),
                "caption": ds.get("caption"),
                "type": ds.get("type", ""),
                "connection_type": ds.get("connection-type", ""),
                "class": ds.get("class", ""),
                "version": ds.get("version", ""),
            }

            # Extract measures, dimensions, parameters
            data["measures"] = [
                self.extract_measure(col)
                for col in ds.findall(".//column[@role='measure']")
            ]
            data["dimensions"] = [
                self.extract_dimension(col)
                for col in ds.findall(".//column[@role='dimension']")
            ]
            data["parameters"] = [
                self.extract_parameter(col)
                for col in ds.findall(".//column[@param-domain-type]")
            ]

            # Extract joins and relationships
            joins_and_rels = self.extract_relationships(ds)
            data["tables"] = joins_and_rels["tables"]
            data["relationships"] = joins_and_rels["relationships"]

            # Extract connections - handle both direct connections and named connections
            connections = []

            # First, check for named connections (federated connections)
            for named_conn in ds.findall(".//named-connection"):
                conn_element = named_conn.find("connection")
                if conn_element is not None:
                    conn_data = self.extract_connection(conn_element)
                    # Use the named-connection's name and caption
                    conn_data["name"] = named_conn.get("name", "")
                    conn_data["caption"] = named_conn.get("caption", "")
                    connections.append(conn_data)

            # Then, check for direct connections (not nested in named-connection)
            for conn in ds.findall(".//connection"):
                # Skip connections that are already handled as named connections
                if conn.getparent().tag != "named-connection":
                    connections.append(self.extract_connection(conn))

            data["connections"] = connections

            datasources.append(data)

        return datasources

    def get_tables(self, datasource: Element) -> List[Dict]:
        """Get all tables from a datasource.

        Args:
            datasource: Datasource element

        Returns:
            List of table info dictionaries
        """
        tables = []

        # Look for tables in both direct relations and object-graph
        for relation in datasource.findall(".//relation[@type='table']"):
            connection = relation.get("connection")
            name = relation.get("name")
            table = relation.get("table")
            if connection and table:
                tables.append({"connection": connection, "name": name, "table": table})

        # Look for tables in object-graph
        for rel in datasource.findall(
            ".//object-graph//object/properties/relation[@type='table']"
        ):
            connection = rel.get("connection")
            name = rel.get("name")
            table = rel.get("table")
            if connection and table:
                table_data = {"connection": connection, "name": name, "table": table}
                if table_data not in tables:
                    tables.append(table_data)

        return tables

    def extract_table_info(self, element: Element) -> Optional[Dict]:
        """Extract table information from a relation element.

        Args:
            element: Relation element to process

        Returns:
            Dict with table info or None if not a table
        """
        if element.get("type") != "table":
            return None

        connection = element.get("connection")
        name = element.get("name")
        table = element.get("table")

        if not (connection and table):
            return None

        return {"connection": connection, "name": name, "table": table}

    def extract_physical_join(self, element: Element) -> Optional[Dict]:
        """Extract physical join information from a relation element.

        Args:
            element: Relation element to process

        Returns:
            Dict with join info or None if not a join
        """
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
        """Extract logical join information from a relationship element.

        Args:
            element: Relationship element to process

        Returns:
            Dict with join info or None if invalid
        """
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
            # endpoint -> relationship -> relationships -> object-graph
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
        """Extract all relationships from a datasource element.

        Args:
            datasource: Datasource element to process

        Returns:
            Dict with tables and relationships
        """
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

    def get_all_elements(self, root: Element) -> List[Dict]:
        """Get all elements from workbook for handler processing.

        Args:
            root: Root element of workbook

        Returns:
            List of element dictionaries with type and raw data
        """
        elements = []

        # Get all datasources
        for datasource in root.findall(".//datasource"):
            # Build table name mapping from metadata records
            table_mapping = self._build_table_mapping(datasource)

            # Build alias resolution mapping
            alias_mapping = self._build_alias_mapping(datasource)

            # Add measures
            for col in datasource.findall(".//column[@role='measure']"):
                measure_data = self.extract_measure(col)
                # Add table association from metadata using raw name
                raw_name = measure_data["raw_name"].strip("[]")
                table_name = table_mapping.get(raw_name)
                # Resolve alias to actual table name
                measure_data["table_name"] = self._resolve_table_alias(
                    table_name, alias_mapping
                )
                elements.append({"type": "measure", "data": measure_data})

            # Also add measures from metadata records with aggregation (for files like Book7)
            # This handles cases where measures exist in metadata but not as column elements
            existing_measure_names = {
                col.get("name", "").strip("[]")
                for col in datasource.findall(".//column[@role='measure']")
            }

            for metadata in datasource.findall(".//metadata-record[@class='column']"):
                aggregation_elem = metadata.find("aggregation")
                local_name_elem = metadata.find("local-name")
                remote_name_elem = metadata.find("remote-name")
                local_type_elem = metadata.find("local-type")

                if (
                    aggregation_elem is not None
                    and aggregation_elem.text == "Sum"
                    and local_name_elem is not None
                    and remote_name_elem is not None
                ):
                    local_name = local_name_elem.text
                    remote_name = remote_name_elem.text
                    local_type = (
                        local_type_elem.text if local_type_elem is not None else "real"
                    )

                    if local_name and remote_name:
                        clean_local_name = local_name.strip("[]")
                        # Skip if this measure already exists as a column element
                        if clean_local_name not in existing_measure_names:
                            # Create measure data from metadata
                            measure_data = {
                                "name": local_name,
                                "raw_name": local_name,
                                "role": "measure",
                                "datatype": local_type,
                                "aggregation": "sum",
                                "number_format": None,
                                "label": remote_name,  # Use remote name as label
                                "table_name": self._resolve_table_alias(
                                    table_mapping.get(clean_local_name), alias_mapping
                                ),
                            }
                            elements.append({"type": "measure", "data": measure_data})

            # Add dimensions
            for col in datasource.findall(".//column[@role='dimension']"):
                dimension_data = self.extract_dimension(col)
                # Add table association from metadata using raw name
                raw_name = dimension_data["raw_name"].strip("[]")
                table_name = table_mapping.get(raw_name)
                # Resolve alias to actual table name
                dimension_data["table_name"] = self._resolve_table_alias(
                    table_name, alias_mapping
                )
                # Add SQL column name from metadata records
                dimension_data["sql_column"] = self._get_sql_column_name(
                    datasource, raw_name
                )
                elements.append({"type": "dimension", "data": dimension_data})

            # Add parameters
            for col in datasource.findall(".//column[@param-domain-type]"):
                elements.append(
                    {"type": "parameter", "data": self.extract_parameter(col)}
                )

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

    def _build_table_mapping(self, datasource: Element) -> Dict[str, str]:
        """Build mapping from column names to table names using metadata records.

        Args:
            datasource: Datasource element containing metadata records

        Returns:
            Dict mapping column names to table names
        """
        table_mapping = {}

        # Extract table associations from metadata records
        for metadata in datasource.findall(".//metadata-record[@class='column']"):
            local_name_elem = metadata.find("local-name")
            parent_name_elem = metadata.find("parent-name")

            if local_name_elem is not None and parent_name_elem is not None:
                # local-name contains the column name like [title] or [id (credits)]
                column_name = local_name_elem.text
                # parent-name contains the table name like [movies_data] or [credits]
                table_name = parent_name_elem.text

                if column_name and table_name:
                    # Strip brackets from both names
                    clean_column_name = column_name.strip("[]")
                    clean_table_name = table_name.strip("[]")

                    # Map column name to table name
                    table_mapping[clean_column_name] = clean_table_name

        # Also create mapping for all actual column names from datasource
        for col in datasource.findall(".//column"):
            col_name = col.get("name", "")
            if col_name:
                # Strip brackets from column name
                clean_col_name = col_name.strip("[]")

                # Skip if we already have an exact match from metadata records
                if clean_col_name in table_mapping:
                    continue

                # Try to find matching metadata record
                for metadata in datasource.findall(
                    ".//metadata-record[@class='column']"
                ):
                    local_name_elem = metadata.find("local-name")
                    parent_name_elem = metadata.find("parent-name")

                    if local_name_elem is not None and parent_name_elem is not None:
                        metadata_col_name = local_name_elem.text
                        if metadata_col_name:
                            metadata_clean_name = metadata_col_name.strip("[]")

                            # Check if this column matches the metadata record
                            # Handle cases like [adult (movies_data2)] matching [adult]
                            if (
                                clean_col_name == metadata_clean_name
                                or clean_col_name.startswith(metadata_clean_name + " (")
                                or metadata_clean_name.startswith(clean_col_name + " (")
                            ):
                                table_name = parent_name_elem.text
                                if table_name:
                                    clean_table_name = table_name.strip("[]")
                                    table_mapping[clean_col_name] = clean_table_name
                                    break

        return table_mapping

    def _build_alias_mapping(self, datasource: Element) -> Dict[str, str]:
        """Build mapping from table aliases to actual table names.

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

    def _get_sql_column_name(self, datasource: Element, column_name: str) -> str:
        """Get the SQL column name from metadata records.

        Args:
            datasource: Datasource element containing metadata records
            column_name: Column name to look up

        Returns:
            SQL column name or the original column name if not found
        """
        # Look for metadata record with matching local-name
        for metadata in datasource.findall(".//metadata-record[@class='column']"):
            local_name_elem = metadata.find("local-name")
            if local_name_elem is not None:
                local_name = local_name_elem.text
                if local_name and local_name.strip("[]") == column_name:
                    # Found matching metadata record, get remote-alias
                    remote_alias_elem = metadata.find("remote-alias")
                    if remote_alias_elem is not None and remote_alias_elem.text:
                        return remote_alias_elem.text

        # If no metadata found, return the original column name
        return column_name
