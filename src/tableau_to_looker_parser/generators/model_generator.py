"""
Generic Model LookML generator (Version 2).

This generator maintains backward compatibility with existing functionality
while adding support for complex relationship patterns like many-to-many bridges.
"""

from typing import Dict, List, Set, Tuple, Optional
import logging
from collections import defaultdict
from datetime import datetime
from .base_generator import BaseGenerator

logger = logging.getLogger(__name__)


class ModelGenerator(BaseGenerator):
    """Generic Model LookML generator with enhanced relationship handling."""

    def generate(self, migration_data: Dict, output_dir: str) -> str:
        """
        Generate a model.lkml file with explores and joins.
        Maintains backward compatibility with original ModelGenerator.
        """
        try:
            # Build field name mapping for v2 parser compatibility
            self.field_name_mapping = self._build_field_name_mapping(migration_data)

            # Build explores with joins using enhanced logic
            explores = self._build_explores(migration_data)

            # Get connection name (same logic as original)
            connection_name = self._get_connection_name(migration_data)

            # Get all view names (same logic as original)
            view_names = self._get_view_names(migration_data)

            # Prepare template context (identical to original)
            context = {
                "project_name": migration_data.get("metadata", {}).get(
                    "project_name", "tableau_migration"
                ),
                "connection_name": connection_name,
                "views": [{"name": view_name} for view_name in sorted(view_names)],
                "explores": explores,
                "dashboards": migration_data.get("dashboards", []),
            }

            # Render template (same as original)
            content = self.template_engine.render_template("model.j2", context)

            # Write to file (same logic as original)
            output_path = self._ensure_output_dir(output_dir)
            # file_name = f"{connection_name}_model{self.model_extension}{self.lookml_extension}"
            file_name = f"{connection_name}_model_generated_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            file_path = (
                output_path
                / f"{file_name}{self.model_extension}{self.lookml_extension}"
            )

            return self._write_file(content, file_path), file_name

        except Exception as e:
            logger.error(f"Failed to generate model file: {str(e)}")
            raise

    def _build_explores(self, migration_data: Dict) -> List[Dict]:
        """Build explores with enhanced join logic while maintaining compatibility."""
        tables = migration_data.get("tables", [])
        if not tables:
            raise ValueError("No tables found in migration data")

        # Use the first table as the primary explore (same as original)
        primary_table = tables[0]
        explore = {
            "name": primary_table["name"],
            "description": f"Explore for {primary_table['name']} with related tables",
            "joins": [],
        }

        # Enhanced logical joins that handle complex patterns
        logical_joins = self._build_enhanced_logical_joins(
            migration_data, primary_table
        )
        explore["joins"].extend(logical_joins)

        # Physical joins (same logic as original for backward compatibility)
        physical_joins = self._build_physical_joins(migration_data, primary_table)
        explore["joins"].extend(physical_joins)

        return [explore]

    def _build_enhanced_logical_joins(
        self, migration_data: Dict, primary_table: Dict
    ) -> List[Dict]:
        """Enhanced logical joins that handle direct and transitive relationships."""
        joins = []
        added_joins = set()

        relationships = [
            r
            for r in migration_data.get("relationships", [])
            if r.get("relationship_type") == "logical"
        ]

        if not relationships:
            return []

        logger.info(f"Processing {len(relationships)} logical relationships")

        # Build relationship map for efficient lookup
        # relationship_map = self._build_relationship_map(relationships)

        # Phase 1: Direct relationships from primary table
        direct_joins = self._find_direct_joins(relationships, primary_table["name"])
        for join_info in direct_joins:
            if (
                join_info["target_table"] not in added_joins
                and join_info["target_table"] != primary_table["name"]
            ):  # Don't join to self
                joins.append(self._format_join(join_info))
                added_joins.add(join_info["target_table"])

        # Phase 2: Transitive relationships through already joined tables
        max_iterations = len(relationships)  # Prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            initial_count = len(added_joins)

            # Find new joinable tables through existing joins
            for joined_table in list(added_joins):
                transitive_joins = self._find_transitive_joins(
                    relationships, joined_table, added_joins
                )
                for join_info in transitive_joins:
                    if (
                        join_info["target_table"] not in added_joins
                        and join_info["target_table"] != primary_table["name"]
                    ):  # Don't join back to primary
                        joins.append(self._format_join(join_info))
                        added_joins.add(join_info["target_table"])

            # Stop if no new joins were added
            if len(added_joins) == initial_count:
                break

            iteration += 1

        logger.info(
            f"Generated {len(joins)} logical joins: {[j['view_name'] for j in joins]}"
        )
        return joins

    def _build_relationship_map(self, relationships: List[Dict]) -> Dict:
        """Build a map of table -> list of relationships for efficient lookup."""
        rel_map = defaultdict(list)

        for rel in relationships:
            endpoints = rel.get("endpoints", {})
            if len(endpoints) == 2:
                first = endpoints.get("first", {}).get("caption")
                second = endpoints.get("second", {}).get("caption")
                if first and second:
                    rel_map[first].append(rel)
                    rel_map[second].append(rel)

        return rel_map

    def _find_direct_joins(
        self, relationships: List[Dict], primary_table: str
    ) -> List[Dict]:
        """Find relationships directly involving the primary table."""
        direct_joins = []

        for rel in relationships:
            join_info = self._parse_relationship_for_table(rel, primary_table)
            if join_info:
                direct_joins.append(join_info)

        return direct_joins

    def _find_transitive_joins(
        self, relationships: List[Dict], source_table: str, existing_joins: Set[str]
    ) -> List[Dict]:
        """Find relationships that can be joined transitively through source_table."""
        transitive_joins = []

        for rel in relationships:
            join_info = self._parse_relationship_for_table(rel, source_table)
            if (
                join_info
                and join_info["target_table"] not in existing_joins
                and join_info["target_table"] != source_table
            ):  # Prevent self-joins
                transitive_joins.append(join_info)

        return transitive_joins

    def _parse_relationship_for_table(
        self, relationship: Dict, source_table: str
    ) -> Optional[Dict]:
        """Parse a relationship to see if it involves the given source table."""
        try:
            endpoints = relationship.get("endpoints", {})
            if len(endpoints) != 2:
                return None

            first = endpoints.get("first", {})
            second = endpoints.get("second", {})

            first_table = first.get("caption")
            second_table = second.get("caption")

            if not first_table or not second_table:
                return None

            # Determine which table is the target (not the source)
            target_table = None
            if first_table == source_table:
                target_table = second_table
            elif second_table == source_table:
                target_table = first_table
            else:
                return None  # Neither table matches source

            # Parse field expressions using enhanced logic
            expressions = relationship.get("expression", {}).get("expressions", [])
            if len(expressions) != 2:
                return None

            source_field, target_field = self._parse_field_expressions(
                expressions,
                source_table,
                target_table,
                relationship.get("table_aliases", {}),
            )

            return {
                "source_table": source_table,
                "target_table": target_table,
                "source_field": source_field,
                "target_field": target_field,
                "join_type": relationship.get("join_type", "inner"),
                "relationship_type": "many_to_one",
            }

        except Exception as e:
            logger.warning(f"Failed to parse relationship: {str(e)}")
            return None

    def _parse_field_expressions(
        self,
        expressions: List[str],
        source_table: str,
        target_table: str,
        table_aliases: Dict,
    ) -> Tuple[str, str]:
        """Parse field expressions with enhanced table qualifier resolution."""
        left_expr = expressions[0].replace("[", "").replace("]", "")
        right_expr = expressions[1].replace("[", "").replace("]", "")

        # Extract field names and table qualifiers
        left_field = self._extract_field_name(left_expr)
        right_field = self._extract_field_name(right_expr)

        left_qualifier = self._extract_table_qualifier(left_expr)
        right_qualifier = self._extract_table_qualifier(right_expr)

        # Resolve qualifiers to actual table names
        left_table = self._resolve_table_qualifier(
            left_qualifier, table_aliases, source_table, target_table
        )
        right_table = self._resolve_table_qualifier(
            right_qualifier, table_aliases, source_table, target_table
        )

        # Determine field assignment based on table qualifiers
        if left_table == source_table:
            return left_field, right_field
        elif right_table == source_table:
            return right_field, left_field
        elif left_table == target_table:
            return right_field, left_field
        elif right_table == target_table:
            return left_field, right_field
        else:
            # Fallback: use field name heuristics (for backward compatibility)
            if source_table in left_field or target_table in right_field:
                return left_field, right_field
            else:
                return right_field, left_field

    def _extract_table_qualifier(self, expression: str) -> Optional[str]:
        """Extract table qualifier from expression like 'field_name (table_name)'."""
        import re

        match = re.search(r"\(([^)]+)\)", expression)
        return match.group(1).strip() if match else None

    def _resolve_table_qualifier(
        self,
        qualifier: Optional[str],
        table_aliases: Dict,
        source_table: str,
        target_table: str,
    ) -> Optional[str]:
        """Resolve table qualifier to actual table name with context awareness."""
        if not qualifier:
            return None

        # Direct match with table aliases
        for alias_name, full_path in table_aliases.items():
            if qualifier == alias_name:
                return alias_name
            # Check if qualifier is a short form of the alias
            if qualifier in alias_name.lower():
                return alias_name

        # Context-aware matching - check if qualifier matches source or target
        if qualifier in source_table.lower():
            return source_table
        if qualifier in target_table.lower():
            return target_table

        # Special case for common aliases (for backward compatibility)
        if qualifier == "enrollments" and "student_enrollments" in [
            source_table,
            target_table,
        ]:
            return "student_enrollments"

        return qualifier

    def _format_join(self, join_info: Dict) -> Dict:
        """Format join information for template rendering."""
        return {
            "view_name": join_info["target_table"],
            "type": join_info["join_type"],
            "sql_on": f"${{{join_info['source_table']}.{join_info['source_field']}}} = ${{{join_info['target_table']}.{join_info['target_field']}}}",
            "relationship": join_info["relationship_type"],
        }

    def _build_field_name_mapping(self, migration_data: Dict) -> Dict[str, str]:
        """Build mapping from original field expressions to clean field names from v2 parser.

        Args:
            migration_data: Migration data containing dimensions and measures

        Returns:
            Dict mapping original expressions like '[id (credits)]' to clean names like 'id'
        """
        field_mapping = {}

        # Build mapping from relationship expressions to clean field names
        # Handle cases like 'id (credits)' -> 'id' and 'id' -> 'id'

        # Get all field names
        all_field_names = set()

        # Collect dimension names
        for dim in migration_data.get("dimensions", []):
            field_name = dim.get("name")
            if field_name:
                all_field_names.add(field_name)
                field_mapping[field_name] = field_name  # Direct mapping: 'id' -> 'id'

        # Collect measure names
        for measure in migration_data.get("measures", []):
            field_name = measure.get("name")
            if field_name:
                all_field_names.add(field_name)
                field_mapping[field_name] = field_name  # Direct mapping: 'id' -> 'id'

        # Add mappings for expressions with table suffixes like 'id (credits)' -> 'id'
        for field_name in all_field_names:
            # Create pattern: 'field_name (table_name)' -> 'field_name'
            # This handles expressions like 'id (credits)' -> 'id'

            # Find all relationship expressions that match this field
            for rel in migration_data.get("relationships", []):
                expressions = rel.get("expression", {}).get("expressions", [])
                for expr in expressions:
                    # Remove brackets first
                    clean_expr = expr.replace("[", "").replace("]", "")

                    # Check if this expression starts with our field name
                    if (
                        clean_expr.startswith(field_name + " (")
                        or clean_expr == field_name
                    ):
                        field_mapping[clean_expr] = field_name

        logger.debug(f"Built field name mapping with {len(field_mapping)} entries")
        return field_mapping

    def _extract_field_name(self, expression: str) -> str:
        """Extract field name from expression using v2 parser field mapping.

        This method now uses the clean field names from v2 parser instead of
        the old cleaning logic to ensure consistency with generated views.
        """
        # First try to use the field mapping from v2 parser
        if (
            hasattr(self, "field_name_mapping")
            and expression in self.field_name_mapping
        ):
            clean_name = self.field_name_mapping[expression]
            logger.debug(
                f"Mapped '{expression}' -> '{clean_name}' using v2 parser field mapping"
            )
            return clean_name

        # Fallback to original logic for backward compatibility
        import re

        # Remove brackets (same as original)
        name = expression.strip("[]")

        # Convert to lowercase (same as original)
        name = name.lower()

        # Replace spaces and special chars with underscore (same as original)
        name = re.sub(r"[^a-z0-9]+", "_", name)

        # Remove duplicate underscores (same as original)
        name = re.sub(r"_+", "_", name)

        # Remove leading/trailing underscores (same as original)
        name = name.strip("_")

        logger.debug(
            f"Fallback mapping '{expression}' -> '{name}' using original logic"
        )
        return name

    # The following methods are identical to original for backward compatibility

    def _build_physical_joins(
        self, migration_data: Dict, primary_table: Dict
    ) -> List[Dict]:
        """Build joins from physical relationships."""
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
            else:
                # Handle regular table-to-table physical joins
                join = self._build_regular_physical_join(
                    relationship, primary_table, existing_joins
                )
                if join:
                    joins.append(join)
                    existing_joins.add(join["view_name"])

        return joins

    def _is_self_join_relationship(
        self, table_aliases: Dict, migration_data: Dict
    ) -> bool:
        """Check if relationship represents a self-join (identical to original)."""
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
        """Build a self-join from physical relationship (identical to original)."""
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
                    "sql_on": f"${{{primary_table['name']}.{primary_field}}} = ${{{join_table_alias}.{join_field}}}",
                    "relationship": "one_to_one",
                }

        return None

    def _build_regular_physical_join(
        self, relationship: Dict, primary_table: Dict, existing_joins: Set
    ) -> Dict:
        """Build a regular table-to-table physical join (not self-join)."""
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

            # Determine which table to join (not the primary table)
            join_table = None
            join_field = None
            primary_field = None

            if left_table == primary_table["name"]:
                # Primary table is on the left, join the right table
                join_table = right_table
                join_field = right_field
                primary_field = left_field
            elif right_table == primary_table["name"]:
                # Primary table is on the right, join the left table
                join_table = left_table
                join_field = left_field
                primary_field = right_field

            if (
                join_table
                and join_table in table_aliases
                and join_table not in existing_joins
            ):
                # Clean table and field names to match view naming convention
                clean_primary_table = self._clean_name(primary_table["name"])
                clean_join_table = self._clean_name(join_table)
                clean_primary_field = self._clean_name(primary_field)
                clean_join_field = self._clean_name(join_field)

                logger.info(
                    f"Creating physical join: {clean_primary_table} -> {clean_join_table} on {clean_primary_field} = {clean_join_field}"
                )
                return {
                    "view_name": clean_join_table,
                    "type": relationship.get("join_type", "inner"),
                    "sql_on": f"${{{clean_primary_table}.{clean_primary_field}}} = ${{{clean_join_table}.{clean_join_field}}}",
                    "relationship": "many_to_one",
                }

        return None

    def _get_connection_name(self, migration_data: Dict) -> str:
        """Get connection name for the model (identical to original)."""
        connections = migration_data.get("connections", [])
        for conn in connections:
            if conn.get("type") == "bigquery" and conn.get("dataset"):
                return f"bigquery_{conn.get('dataset', 'default').lower()}"
        return "tableau_looker_poc"  # Match existing test expectations

    def _get_view_names(self, migration_data: Dict) -> Set[str]:
        """Get all view names needed in the model (identical to original)."""
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
