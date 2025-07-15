from typing import Dict
from ..handlers.base_handler import BaseHandler


class RelationshipHandler(BaseHandler):
    """Handler for Tableau relationships.

    Handles:
    - Converting raw relationship data to standardized JSON format
    - Join type standardization (inner, outer, etc)
    - Join condition normalization
    - Table reference standardization

    Does NOT handle XML parsing - that's XMLParser's job.
    """

    def __init__(self):
        super().__init__()

    def can_handle(self, data: Dict) -> float:
        """Determine if this handler can process the raw data.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            float: Confidence score between 0.0 and 1.0
        """
        # Check if this is a relationships data structure
        if "relationships" in data and "tables" in data:
            return 1.0

        # Must have relationship_type for individual relationships
        relationship_type = data.get("relationship_type")
        if not relationship_type:
            return 0.0

        # Handle physical joins
        if relationship_type == "physical":
            if data.get("join_type") and data.get("tables"):
                return 1.0

        # Handle logical relationships
        if relationship_type == "logical":
            if data.get("first_endpoint") and data.get("second_endpoint"):
                return 1.0

        return 0.0

    def convert_to_json(self, raw_data: Dict) -> Dict:
        """Convert raw relationship data to schema-compliant JSON.

        Args:
            raw_data: Raw data dict from XMLParser.extract_relationships()

        Returns:
            Dict: Schema-compliant relationship data
        """
        # Check if this is a relationships data structure (from get_all_elements)
        if "relationships" in raw_data and "tables" in raw_data:
            return self.process_datasource(raw_data)

        # Extract expression data
        expr = raw_data["expression"]
        expr_data = {
            "operator": expr["operator"],
            "expressions": sorted(expr["expressions"]),
        }

        # Process based on relationship type
        relationship_type = raw_data["relationship_type"]

        # For physical joins
        if relationship_type == "physical":
            # Extract unique tables and their aliases
            unique_tables = []
            seen_tables = set()
            table_aliases = raw_data.get("table_aliases", {})

            for table_info in raw_data["tables"]:
                table_name = table_info["table"]
                if table_name not in seen_tables:
                    unique_tables.append(table_name)
                    seen_tables.add(table_name)

            return {
                "relationship_type": "physical",
                "join_type": raw_data["join_type"],
                "expression": expr_data,
                "tables": sorted(unique_tables),
                "table_aliases": table_aliases,
            }

        # For logical relationships
        elif relationship_type == "logical":
            first = raw_data["first_endpoint"]
            second = raw_data["second_endpoint"]

            # Create table aliases for logical relationships
            # Extract table alias from expression fields
            table_aliases = {}
            unique_tables = []
            seen_tables = set()

            # Map first endpoint
            first_table = first["table"]
            first_caption = first.get("caption", first_table)
            table_aliases[first_caption] = first_table
            if first_table not in seen_tables:
                unique_tables.append(first_table)
                seen_tables.add(first_table)

            # Map second endpoint
            second_table = second["table"]
            second_caption = second.get("caption", second_table)
            table_aliases[second_caption] = second_table
            if second_table not in seen_tables:
                unique_tables.append(second_table)
                seen_tables.add(second_table)

            # Also try to extract aliases from expression fields
            for expr in expr_data["expressions"]:
                if "(" in expr and ")" in expr:
                    # Expression like "[id (credits)]" -> alias is "credits"
                    parts = expr.strip("[]").split("(")
                    if len(parts) == 2:
                        alias = parts[1].strip(")")
                        # Try to match alias to endpoint
                        if alias == first_caption:
                            table_aliases[alias] = first_table
                        elif alias == second_caption:
                            table_aliases[alias] = second_table
                elif expr.strip("[]") in ["id"]:
                    # Simple field like "[id]" typically refers to first table
                    table_aliases[first_caption] = first_table

            return {
                "relationship_type": "logical",
                "join_type": "inner",  # Logical relationships are always inner joins
                "expression": expr_data,
                "tables": sorted(unique_tables),
                "table_aliases": table_aliases,
                "endpoints": {
                    "first": {
                        "table": first["table"],
                        "connection": first["connection"],
                        "caption": first.get("caption"),
                    },
                    "second": {
                        "table": second["table"],
                        "connection": second["connection"],
                        "caption": second.get("caption"),
                    },
                },
            }

        raise ValueError(f"Unsupported relationship type: {relationship_type}")

    def process_datasource(self, data: Dict) -> Dict:
        """Process raw datasource data to extract standardized relationships.

        Args:
            data: Raw datasource dict from XMLParser.get_datasources()

        Returns:
            Dict: Standardized tables and relationships
        """
        tables = []
        relationships = []

        # Process tables
        if "tables" in data:
            tables.extend(data["tables"])

        # Process relationships
        if "relationships" in data:
            for rel_data in data["relationships"]:
                if self.can_handle(rel_data):
                    json_data = self.convert_to_json(rel_data)
                    relationships.append(json_data)

        # Deduplicate tables
        unique_tables = {}
        for table in tables:
            table_key = f"{table['connection']}:{table['table']}"
            if table_key not in unique_tables:
                unique_tables[table_key] = table

        # Deduplicate relationships
        unique_relationships = {}
        for rel in relationships:
            # Create key from normalized data
            expr = rel.get("expression", {})
            expressions = sorted(expr.get("expressions", []))
            if rel["relationship_type"] == "physical":
                key = f"physical:{rel['join_type']}:{expr['operator']}:{','.join(expressions)}"
            else:
                key = f"logical:{expr['operator']}:{','.join(expressions)}"

            if key not in unique_relationships:
                unique_relationships[key] = rel

        return {
            "tables": list(unique_tables.values()),
            "relationships": list(unique_relationships.values()),
        }
