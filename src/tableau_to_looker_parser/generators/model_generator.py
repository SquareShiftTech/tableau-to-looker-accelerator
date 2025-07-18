"""
Model LookML generator.
"""

from typing import Dict, List, Set
import logging

from .base_generator import BaseGenerator

logger = logging.getLogger(__name__)


class ModelGenerator(BaseGenerator):
    """Generator for model.lkml files."""

    def generate(self, migration_data: Dict, output_dir: str) -> str:
        """
        Generate a model.lkml file with explores and joins.

        Args:
            migration_data: Complete migration data with relationships
            output_dir: Directory to write the file to

        Returns:
            Path to the generated file
        """
        try:
            # Build explores with joins
            explores = self._build_explores(migration_data)

            # Get connection name
            connection_name = self._get_connection_name(migration_data)

            # Get all view names (including self-join aliases)
            view_names = self._get_view_names(migration_data)

            # Prepare template context
            context = {
                "project_name": migration_data.get("metadata", {}).get(
                    "project_name", "tableau_migration"
                ),
                "connection_name": connection_name,
                "views": [{"name": view_name} for view_name in sorted(view_names)],
                "explores": explores,
            }

            # Render template
            content = self.template_engine.render_template("model.j2", context)

            # Write to file
            output_path = self._ensure_output_dir(output_dir)
            file_path = output_path / f"model{self.lookml_extension}"

            return self._write_file(content, file_path)

        except Exception as e:
            logger.error(f"Failed to generate model file: {str(e)}")
            raise

    def _build_explores(self, migration_data: Dict) -> List[Dict]:
        """Build explores with joins from relationships."""
        tables = migration_data.get("tables", [])
        if not tables:
            raise ValueError("No tables found in migration data")

        # Use the first table as the primary explore
        primary_table = tables[0]
        explore = {
            "name": primary_table["name"],
            "description": f"Explore for {primary_table['name']} with related tables",
            "joins": [],
        }

        # Add joins from logical relationships (cross-table joins)
        logical_joins = self._build_logical_joins(migration_data, primary_table)
        explore["joins"].extend(logical_joins)

        # Add joins from physical relationships (self-joins)
        physical_joins = self._build_physical_joins(migration_data, primary_table)
        explore["joins"].extend(physical_joins)

        return [explore]

    def _build_logical_joins(
        self, migration_data: Dict, primary_table: Dict
    ) -> List[Dict]:
        """Build joins from logical relationships."""
        joins = []
        logical_relationships = [
            r
            for r in migration_data.get("relationships", [])
            if r.get("relationship_type") == "logical"
        ]

        for relationship in logical_relationships:
            join_tables = relationship.get("tables", [])
            if len(join_tables) >= 2:
                # Find the join table (not the primary table)
                join_table = self._find_join_table(join_tables, primary_table["name"])

                if join_table:
                    # Build join condition from expressions
                    sql_on = self._build_join_condition(
                        relationship, primary_table["name"], join_table
                    )

                    if sql_on:
                        join = {
                            "view_name": join_table,
                            "type": relationship.get("join_type", "left_outer"),
                            "sql_on": sql_on,
                            "relationship": "many_to_one",
                        }
                        joins.append(join)

        return joins

    def _build_physical_joins(
        self, migration_data: Dict, primary_table: Dict
    ) -> List[Dict]:
        """Build joins from physical relationships (self-joins)."""
        joins = []
        existing_joins = set()

        physical_relationships = [
            r
            for r in migration_data.get("relationships", [])
            if r.get("relationship_type") == "physical"
        ]

        for relationship in physical_relationships:
            table_aliases = relationship.get("table_aliases", {})

            # Check if this is a self-join (multiple aliases for same table)
            if self._is_self_join_relationship(table_aliases, migration_data):
                join = self._build_self_join(
                    relationship, primary_table, existing_joins
                )
                if join:
                    joins.append(join)
                    existing_joins.add(join["view_name"])

        return joins

    def _is_self_join_relationship(
        self, table_aliases: Dict, migration_data: Dict
    ) -> bool:
        """Check if relationship represents a self-join."""
        # Group aliases by their table reference
        table_refs = {}
        for alias, table_ref in table_aliases.items():
            if table_ref not in table_refs:
                table_refs[table_ref] = []
            table_refs[table_ref].append(alias)

        # Self-join if any table has multiple aliases
        return any(len(aliases) > 1 for aliases in table_refs.values())

    def _build_self_join(
        self, relationship: Dict, primary_table: Dict, existing_joins: Set
    ) -> Dict:
        """Build a self-join from physical relationship."""
        table_aliases = relationship.get("table_aliases", {})
        expressions = relationship.get("expression", {}).get("expressions", [])

        if len(expressions) < 2:
            return None

        # Parse expressions to get join fields
        left_expr = expressions[0].replace("[", "").replace("]", "")
        right_expr = expressions[1].replace("[", "").replace("]", "")

        # Extract table.field format
        left_parts = left_expr.split(".")
        right_parts = right_expr.split(".")

        if len(left_parts) >= 2 and len(right_parts) >= 2:
            left_table = left_parts[0]
            left_field = left_parts[1]
            right_table = right_parts[0]
            right_field = right_parts[1]

            # Determine which is the join table (not primary)
            join_table_alias = None
            join_field = None
            primary_field = None

            if left_table != primary_table["name"] and left_table in table_aliases:
                join_table_alias = left_table
                join_field = left_field
                primary_field = right_field
            elif right_table != primary_table["name"] and right_table in table_aliases:
                join_table_alias = right_table
                join_field = right_field
                primary_field = left_field

            if join_table_alias and join_table_alias not in existing_joins:
                return {
                    "view_name": join_table_alias,
                    "type": relationship.get("join_type", "inner"),
                    "sql_on": f"${{{primary_table['name']}}}.{primary_field} = ${{{join_table_alias}}}.{join_field}",
                    "relationship": "one_to_one",
                }

        return None

    def _find_join_table(self, join_tables: List[str], primary_table_name: str) -> str:
        """Find the join table (not the primary table)."""
        for jt in join_tables:
            table_name = jt.split(".")[-1].strip("[]").replace("[", "").replace("]", "")
            if table_name != primary_table_name:
                return table_name
        return None

    def _build_join_condition(
        self, relationship: Dict, primary_table_name: str, join_table: str
    ) -> str:
        """Build SQL join condition from relationship expressions."""
        expressions = relationship.get("expression", {}).get("expressions", [])
        if len(expressions) < 2:
            return None

        # Parse expressions: "[id (credits)]" and "[id]"
        left_expr = expressions[0].replace("[", "").replace("]", "")
        right_expr = expressions[1].replace("[", "").replace("]", "")

        # Extract field names (remove table references in parentheses)
        left_field = self._extract_field_name(left_expr)
        right_field = self._extract_field_name(right_expr)

        # Create proper join condition
        return (
            f"${{{primary_table_name}}}.{right_field} = ${{{join_table}}}.{left_field}"
        )

    def _extract_field_name(self, expression: str) -> str:
        """Extract field name from expression, removing table references."""
        # Handle formats like "id (credits)" -> "id" or "id" -> "id"
        if "(" in expression:
            return expression.split("(")[0].strip()
        return expression.strip().replace(" ", "_").lower()

    def _get_connection_name(self, migration_data: Dict) -> str:
        """Get connection name for the model."""
        connections = migration_data.get("connections", [])
        for conn in connections:
            if conn.get("type") == "bigquery" and conn.get("dataset"):
                return f"bigquery_{conn.get('dataset', 'default').lower()}"
        return "default_connection"

    def _get_view_names(self, migration_data: Dict) -> Set[str]:
        """Get all view names needed in the model."""
        view_names = set()

        # Add actual table names
        for table in migration_data.get("tables", []):
            view_names.add(table["name"])

        # Add self-join aliases using same logic as view generator
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
                                view_names.add(alias)

        return view_names
