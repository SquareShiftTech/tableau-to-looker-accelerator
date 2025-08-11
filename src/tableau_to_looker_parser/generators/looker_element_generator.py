"""
LookerElementGenerator - Generate Looker-native dashboard elements.

Creates clean dashboard elements using YAML detection metadata and worksheet data.
Focuses on essential properties without ECharts complexity.
"""

from typing import Dict, List, Any
import logging
from ..models.worksheet_models import WorksheetSchema, FieldReference
from .filter_processor import FilterProcessor

logger = logging.getLogger(__name__)


class LookerElementGenerator:
    """Generate Looker-native dashboard elements from worksheet schemas."""

    def __init__(self, model_name: str = None, explore_name: str = None):
        """Initialize element generator with model and explore context."""
        self.model_name = model_name or "default_model"
        self.explore_name = explore_name or "default_explore"

        # Unified source mapping configuration
        self.source_mapping = {
            "rows_shelf": {"shelf": "rows"},
            "column_shelf": {"shelf": "columns"},
            "columns_shelf": {"shelf": "columns"},
            "text_marks": {"encodings_contains": "text"},
            "color_marks": {"encodings_contains": "color"},
            "size_marks": {"encodings_contains": "size"},
        }

    def generate_element(
        self, worksheet: WorksheetSchema, position: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a complete Looker-native dashboard element.

        Args:
            worksheet: Validated worksheet schema with fields and visualization
            position: Layout position dict with row, col, width, height

        Returns:
            Dict containing complete dashboard element configuration
        """
        if not worksheet.visualization:
            logger.warning(f"Worksheet {worksheet.name} has no visualization config")
            return self._create_fallback_element(worksheet, position)

        yaml_detection = worksheet.visualization.yaml_detection
        if not yaml_detection:
            logger.warning(f"Worksheet {worksheet.name} has no YAML detection data")
            return self._create_fallback_element(worksheet, position)

        # Get Looker chart type from YAML detection
        looker_chart_type = yaml_detection.get("looker_equivalent", "table")

        # Build element configuration
        element = {
            "title": self._generate_title(worksheet),
            "name": worksheet.clean_name,
            "model": self.model_name,
            "explore": self.explore_name,
            "type": looker_chart_type,
            "fields": self._generate_fields(worksheet),
            "limit": 500,
            "column_limit": 50,
        }

        # Add optional components based on YAML metadata
        pivots = self._generate_pivots(worksheet, yaml_detection)
        if pivots:
            element["pivots"] = pivots

        sorts = self._generate_sorts(worksheet, looker_chart_type)
        if sorts:
            element["sorts"] = sorts

        filters = self._generate_filters(worksheet)
        if filters:
            element["filters"] = filters

        # Add position information
        element.update(position)

        logger.debug(
            f"Generated {looker_chart_type} element for worksheet {worksheet.name}"
        )
        return element

    def _generate_title(self, worksheet: WorksheetSchema) -> str:
        """Generate human-readable title from worksheet title or name as fallback."""
        if worksheet.title and worksheet.title.strip():
            return worksheet.title.strip()
        # Fallback to cleaned worksheet name
        return worksheet.name.replace("_", " ").title()

    def _generate_fields(self, worksheet: WorksheetSchema) -> List[str]:
        """
        Generate fields array using YAML fields_sources and unified mapping.
        """
        yaml_detection = worksheet.visualization.yaml_detection
        if not yaml_detection:
            return []

        fields_sources = yaml_detection.get("fields_sources", [])
        if not fields_sources:
            return []

        fields = []

        # Process each source using unified mapping
        for source in fields_sources:
            source_fields = self._get_fields_by_source(worksheet, source)
            fields.extend(source_fields)

        # Remove duplicates while preserving order
        unique_fields = []
        for field in fields:
            if field not in unique_fields:
                unique_fields.append(field)

        return unique_fields

    def _generate_fields_fallback(self, worksheet: WorksheetSchema) -> List[str]:
        """Fallback field generation when YAML metadata is not available."""
        fields = []

        # Time dimensions first
        time_fields = [f for f in worksheet.fields if self._is_time_field(f)]
        for field in time_fields:
            fields.append(f"{self.explore_name}.{field.name}")

        # Other dimensions
        dimension_fields = [
            f
            for f in worksheet.fields
            if f.role == "dimension" and not self._is_time_field(f)
        ]
        for field in dimension_fields:
            fields.append(f"{self.explore_name}.{field.name}")

        # Measures last
        measure_fields = [f for f in worksheet.fields if f.role == "measure"]
        for field in measure_fields:
            fields.append(f"{self.explore_name}.{field.name}")

        return fields

    def _get_fields_by_source(
        self, worksheet: WorksheetSchema, source: str
    ) -> List[str]:
        """
        Get fields by source using unified mapping configuration.
        """
        fields = []

        if worksheet.clean_name == "cd_st":
            print(f"Worksheet {worksheet.clean_name} has source {source}")

        # Get mapping criteria for this source
        mapping = self.source_mapping.get(source)
        if not mapping:
            logger.warning(f"Unknown source: {source}")
            return fields

        # Process worksheet fields based on mapping criteria
        for field in worksheet.fields:
            if not isinstance(field, FieldReference):
                continue

            field_name = field.name
            if not field_name:
                continue

            # Check if field matches the mapping criteria
            field_matches = False

            # Check shelf criteria
            if "shelf" in mapping:
                field_shelf = field.shelf
                if field_shelf == mapping["shelf"]:
                    field_matches = True

            # Check encodings criteria
            if "encodings_contains" in mapping:
                field_encodings = field.encodings
                if "color" in field_encodings:
                    print(f"Color encoding found in {field.name}")
                if mapping["encodings_contains"] in field_encodings:
                    field_matches = True

            if field_matches:
                # Use derived field name for proper view reference
                derived_field_name = self._get_derived_field_name(field)
                full_field_name = f"{self.explore_name}.{derived_field_name}"
                if full_field_name not in fields:
                    fields.append(full_field_name)

        return fields

    def _get_derived_field_name(self, field: Dict) -> str:
        """
        Get the correct derived field name that matches the generated view.

        Maps Tableau field instances to their corresponding view field names:
        - dimension_group fields: add _date timeframe (rpt_dt -> rpt_dt_date)
        - measure fields: use total_ prefix (sales -> total_sales)
        - regular dimensions: use base name as-is
        """
        field_name = field.name
        role = field.role
        suggested_type = field.suggested_type
        aggregation = field.aggregation

        if suggested_type == "dimension_group":
            # For dimension groups, add appropriate timeframe
            if aggregation and "day" in aggregation.lower():
                return f"{field_name}_date"
            elif aggregation and "hour" in aggregation.lower():
                return f"{field_name}_time"
            else:
                return f"{field_name}_date"  # Default to date
        elif role == "measure":
            # For measures, use total_ prefix
            return f"total_{field_name}"
        else:
            # For regular dimensions, use name as-is
            return field_name

    def _generate_pivots(
        self, worksheet: WorksheetSchema, yaml_detection: Dict[str, Any]
    ) -> List[str]:
        """
        Generate pivots using YAML pivot_field_source and unified mapping.
        """
        # Must have YAML detection
        if not yaml_detection:
            return []

        # Must explicitly require pivots
        if not yaml_detection.get("pivot_required", False):
            return []

        # Must specify pivot field sources
        pivot_field_sources = yaml_detection.get("pivot_field_source", [])
        if not pivot_field_sources:
            return []

        pivots = []

        # Process each pivot source using unified mapping
        for source in pivot_field_sources:
            source_fields = self._get_fields_by_source(worksheet, source)
            selected_pivots = self._apply_pivot_selection_logic(
                source_fields, yaml_detection
            )
            pivots.extend(selected_pivots)

        # Remove duplicates while preserving order
        unique_pivots = []
        for pivot in pivots:
            if pivot not in unique_pivots:
                unique_pivots.append(pivot)

        return unique_pivots

    def _apply_pivot_selection_logic(
        self, source_fields: List[str], yaml_detection: Dict[str, Any]
    ) -> List[str]:
        """
        Apply pivot field selection logic based on YAML configuration.

        Args:
            source_fields: List of field names from a pivot source
            yaml_detection: YAML detection config containing pivot_selection_logic

        Returns:
            List of selected pivot fields based on the logic
        """
        if not source_fields:
            return []

        pivot_selection_logic = yaml_detection.get("pivot_selection_logic")

        # Debug for CD interval specifically
        print(f"   source_fields: {source_fields}")
        print(f"   pivot_selection_logic: {pivot_selection_logic}")
        print(f"   yaml_detection keys: {list(yaml_detection.keys())}")

        if pivot_selection_logic == "all_except_last":
            # For temporal columns: take all fields except the last one
            # Examples:
            # [date, time] -> [date]
            # [year, day, hour] -> [year, day]
            if len(source_fields) > 1:
                selected = source_fields[:-1]
                return selected
            else:
                # If only one field, return it
                return source_fields
        else:
            # Default behavior: return all fields
            return source_fields

    def _generate_sorts(self, worksheet: WorksheetSchema, chart_type: str) -> List[str]:
        """
        Clean implementation: Generate sorts using worksheet handler fields with encodings.
        """
        sorts = []

        if chart_type == "looker_pie":
            # Pie charts: sort by first measure descending
            for field in worksheet.fields:
                if isinstance(field, dict) and field.get("role") == "measure":
                    field_name = field.get("name", "")
                    if field_name:
                        sorts.append(f"{self.explore_name}.{field_name} desc 0")
                        break
        else:
            # Other charts: sort by dimensions then measures
            for field in worksheet.fields:
                if isinstance(field, dict):
                    field_name = field.get("name", "")
                    role = field.get("role", "")
                    if field_name and role in ["dimension", "measure"]:
                        sorts.append(f"{self.explore_name}.{field_name}")

        return sorts

    def _generate_filters(self, worksheet: WorksheetSchema) -> Dict[str, str]:
        """Generate filters from worksheet filter configuration using clean Pydantic processor."""
        if not hasattr(worksheet, "filters") or not worksheet.filters:
            return {}

        # Initialize filter processor with explore context
        filter_processor = FilterProcessor(explore_name=self.explore_name)

        # Convert worksheet filters to LookML element filters
        filters = filter_processor.process_worksheet_filters(worksheet.filters)

        # Filter out empty values to avoid validation issues
        return {k: v for k, v in filters.items() if v.strip()}

    def _create_fallback_element(
        self, worksheet: WorksheetSchema, position: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create basic element when YAML detection is missing."""
        logger.warning(f"Creating fallback element for worksheet {worksheet.name}")

        return {
            "title": self._generate_title(worksheet),
            "name": worksheet.clean_name,
            "model": self.model_name,
            "explore": self.explore_name,
            "type": "table",  # Safe fallback
            "fields": self._generate_fields(worksheet),
            "limit": 500,
            "column_limit": 50,
            **position,
        }

    def set_model_explore(self, model_name: str, explore_name: str):
        """Update model and explore names for element generation."""
        self.model_name = model_name
        self.explore_name = explore_name
        logger.debug(f"Updated element generator context: {model_name}.{explore_name}")
