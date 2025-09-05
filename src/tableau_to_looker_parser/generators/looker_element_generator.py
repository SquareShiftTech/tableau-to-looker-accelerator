"""
LookerElementGenerator - Generate Looker-native dashboard elements.

Creates clean dashboard elements using YAML detection metadata and worksheet data.
Focuses on essential properties without ECharts complexity.
"""

from typing import Dict, List, Any
import logging
from ..models.worksheet_models import WorksheetSchema, FieldReference
from .filter_processor import FilterProcessor
from ..converters.chart_styling_engine import ChartStylingEngine

logger = logging.getLogger(__name__)


class LookerElementGenerator:
    """Generate Looker-native dashboard elements from worksheet schemas."""

    def __init__(self, model_name: str = None, explore_name: str = None):
        """Initialize element generator with model and explore context."""
        self.model_name = model_name or "default_model"
        self.explore_name = explore_name or "default_explore"

        # Initialize styling engine
        self.styling_engine = ChartStylingEngine()

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
        self,
        worksheet: WorksheetSchema,
        position: Dict[str, Any],
        view_mappings: Dict = None,
    ) -> Dict[str, Any]:
        """
        Generate a complete Looker-native dashboard element.

        Args:
            worksheet: Validated worksheet schema with fields and visualization
            position: Layout position dict with row, col, width, height

        Returns:
            Dict containing complete dashboard element configuration
        """
        if worksheet.name == "byTypeS apple":
            print(f"Worksheet {worksheet.name} has data: {worksheet}")

        if not worksheet.visualization:
            logger.warning(f"Worksheet {worksheet.name} has no visualization config")
            return self._create_fallback_element(worksheet, position, view_mappings)

        yaml_detection = worksheet.visualization.yaml_detection
        if not yaml_detection:
            logger.warning(f"Worksheet {worksheet.name} has no YAML detection data")
            return self._create_fallback_element(worksheet, position, view_mappings)

        # Get Looker chart type from YAML detection
        looker_chart_type = yaml_detection.get("looker_equivalent", "table")
        # new function for worksheet.fields to get the correct field name
        # 1. Take the view mapping  construct the 3 json dimension,measure,calculated_fields
        # dimension json strcure : [datasource_id]: {"local_name": self.clean_name(name) }
        # measure json strcure : [datasource_id]: {"local_name": self.clean_name(name),"aggregation": "sum"  }
        # calculated_fields json strcure : [datasource_id]: {"local_name": self.clean_name(name) }

        # Generate fields using view mappings if available
        # if view_mappings:
        field_mappings = self._construct_field_mappings(view_mappings)
        status = self._update_worksheet_fields_with_view_mappings(
            worksheet, field_mappings
        )
        if status == "Success":
            logger.debug(
                f"Updated worksheet fields with view mappings: {worksheet.name}"
            )

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

        if worksheet.name == "CD detail":
            print(f"Worksheet {worksheet.name} has styling: {worksheet.styling}")

        # Add optional components based on YAML metadata
        pivots = self._generate_pivots(worksheet, yaml_detection)
        if pivots:
            element["pivots"] = pivots

        sorts = self._generate_sorts(
            worksheet, element["fields"], pivots, looker_chart_type
        )
        if sorts:
            element["sorts"] = sorts

        filters = self._generate_filters(worksheet, field_mappings)
        if filters:
            element["filters"] = filters

        # Apply styling configuration using extracted Tableau styling data
        if hasattr(worksheet, "styling") and worksheet.styling:
            styled_element = self.styling_engine.apply_styling(
                element, worksheet.styling, looker_chart_type
            )
            element = styled_element
            logger.debug(f"Applied styling configuration to {worksheet.name}")

        # Add position information
        element.update(position)

        try:
            if bool(getattr(worksheet.visualization, "stacked", False)):
                element["stacking"] = "normal"
        except Exception:
            pass

        logger.debug(
            f"Generated {looker_chart_type} element for worksheet {worksheet.name}"
        )
        return element

    def _generate_title(self, worksheet: WorksheetSchema) -> str:
        """Generate human-readable title from worksheet name, avoiding placeholder titles."""
        # Check if title is a placeholder and use name instead
        if (
            worksheet.title
            and worksheet.title.strip()
            and worksheet.title.strip() != "<Sheet Name>"
        ):
            return worksheet.title.strip()
        # Use worksheet name as the primary source for titles
        return worksheet.name

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
                if self._get_derived_field_name(field):
                    derived_field_name = self._get_derived_field_name(field)
                else:
                    derived_field_name = field.view_mapping_name

                full_field_name = f"{self.explore_name}.{derived_field_name}"
                if full_field_name not in fields:
                    fields.append(full_field_name)

        return fields

    def _get_derived_field_name_with_view_mappings(
        self, field: Dict, view_mappings: Dict = None
    ) -> str:
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
            elif aggregation and "month" in aggregation.lower():
                return f"{field_name}_month"
            elif aggregation and "year" in aggregation.lower():
                return f"{field_name}_year"
            elif aggregation and "hour" in aggregation.lower():
                return f"{field_name}_hour_formatted"
            else:
                return f"{field_name}_date"  # Default to date
        elif role == "measure":
            # For measures, use total_ prefix
            return f"total_{field_name}"
        else:
            # For regular dimensions, use name as-is
            return field_name

    def _get_derived_field_name(self, field: Dict) -> str:
        """
        Get the correct derived field name that matches the generated view.

        Maps Tableau field instances to their corresponding view field names:
        - dimension_group fields: add _date timeframe (rpt_dt -> rpt_dt_date)
        - measure fields: use total_ prefix (sales -> total_sales)
        - regular dimensions: use base name as-is
        """
        field_name = field.name
        suggested_type = field.suggested_type
        aggregation = field.aggregation

        if suggested_type == "dimension_group":
            # For dimension groups, add appropriate timeframe
            if aggregation and "day" in aggregation.lower():
                return f"{field_name}_date"
            elif aggregation and "month" in aggregation.lower():
                return f"{field_name}_month"
            elif aggregation and "year" in aggregation.lower():
                return f"{field_name}_year"
            elif aggregation and "hour" in aggregation.lower():
                return f"{field_name}_hour_formatted"
            else:
                return f"{field_name}_date"  # Default to date
        else:
            return None

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

    def _generate_sorts(
        self,
        worksheet: WorksheetSchema,
        fields: List[str],
        pivot_fields: List[str],
        chart_type: str,
    ) -> List[str]:
        """
        Priority-based sort generation: TIME → PIVOT → ROW DIMENSIONS → MEASURES

        Args:
            worksheet: Worksheet schema with field metadata
            fields: All fields being used in the visualization
            pivot_fields: Fields that are pivoted (become column headers)
            chart_type: Chart type for special sorting rules
        """
        return self._apply_sort_priority_hierarchy(
            worksheet, fields, pivot_fields, chart_type
        )

    def _apply_sort_priority_hierarchy(
        self,
        worksheet: WorksheetSchema,
        fields: List[str],
        pivot_fields: List[str],
        chart_type: str,
    ) -> List[str]:
        """
        Apply sort priority hierarchy using actual field metadata: TIME → PIVOT → ROW DIMENSIONS → MEASURES

        Args:
            worksheet: Worksheet schema with field metadata
            fields: All fields being used in the visualization
            pivot_fields: Fields that are pivoted (become column headers)
            chart_type: Chart type for special sorting rules

        Returns:
            Ordered list of sort specifications
        """
        sorts = []
        processed_fields = set()

        # 1. TIME FIELDS (chronological flow) - highest priority
        time_fields = self._get_time_fields_from_list(worksheet, fields)
        for field in time_fields:
            sorts.append(field)
            processed_fields.add(field)

        # 2. PIVOT FIELDS (column organization) - second priority
        for field in pivot_fields:
            if field not in processed_fields:
                sorts.append(field)
                processed_fields.add(field)

        # 3. ROW DIMENSIONS (row organization) - third priority
        row_dimension_fields = self._get_row_dimension_fields_from_list(
            worksheet, fields
        )
        for field in row_dimension_fields:
            if field not in processed_fields:
                sorts.append(field)
                processed_fields.add(field)

        return sorts

    def _get_time_fields_from_list(
        self, worksheet: WorksheetSchema, fields: List[str]
    ) -> List[str]:
        """Get time fields by matching with worksheet field metadata."""
        time_fields = []
        for field_name in fields:
            # Extract field name without explore prefix
            clean_field_name = field_name.split(".")[-1]

            # Find matching worksheet field
            for worksheet_field in worksheet.fields:
                if (
                    hasattr(worksheet_field, "view_mapping_name")
                    and worksheet_field.view_mapping_name
                    and worksheet_field.view_mapping_name in clean_field_name
                ):
                    if (
                        hasattr(worksheet_field, "suggested_type")
                        and worksheet_field.suggested_type == "dimension_group"
                    ):
                        time_fields.append(field_name)
                        break
                    elif (
                        hasattr(worksheet_field, "role")
                        and worksheet_field.role == "dimension"
                        and hasattr(worksheet_field, "datatype")
                        and worksheet_field.datatype in ["date", "datetime"]
                    ):
                        time_fields.append(field_name)
                        break

        return time_fields

    def _get_measure_fields_from_list(
        self, worksheet: WorksheetSchema, fields: List[str]
    ) -> List[str]:
        """Get measure fields by matching with worksheet field metadata."""
        measure_fields = []
        for field_name in fields:
            # Extract field name without explore prefix
            clean_field_name = field_name.split(".")[-1]

            # Find matching worksheet field
            for worksheet_field in worksheet.fields:
                if (
                    hasattr(worksheet_field, "name")
                    and worksheet_field.name in clean_field_name
                ):
                    if (
                        hasattr(worksheet_field, "role")
                        and worksheet_field.role == "measure"
                    ):
                        measure_fields.append(field_name)
                        break

        return measure_fields

    def _get_row_dimension_fields_from_list(
        self, worksheet: WorksheetSchema, fields: List[str]
    ) -> List[str]:
        """Get row dimension fields by matching with worksheet field metadata."""
        row_dimension_fields = []
        for field_name in fields:
            # Extract field name without explore prefix
            clean_field_name = field_name.split(".")[-1]

            # Find matching worksheet field
            for worksheet_field in worksheet.fields:
                if (
                    hasattr(worksheet_field, "view_mapping_name")
                    and worksheet_field.view_mapping_name
                    and worksheet_field.view_mapping_name in clean_field_name
                ):
                    if (
                        hasattr(worksheet_field, "role")
                        and worksheet_field.role == "dimension"
                        and hasattr(worksheet_field, "shelf")
                        and worksheet_field.shelf == "rows"
                        and not (
                            hasattr(worksheet_field, "suggested_type")
                            and worksheet_field.suggested_type == "dimension_group"
                        )
                    ):
                        row_dimension_fields.append(field_name)
                        break

        return row_dimension_fields

    def _generate_filters(
        self, worksheet: WorksheetSchema, field_mappings: Dict
    ) -> Dict[str, str]:
        """Generate filters from worksheet filter configuration using clean Pydantic processor."""
        if not hasattr(worksheet, "filters") or not worksheet.filters:
            return {}

        # Initialize filter processor with explore context
        filter_processor = FilterProcessor(explore_name=self.explore_name)

        # Convert worksheet filters to LookML element filters, excluding calculated fields
        filters = filter_processor.process_worksheet_filters(
            worksheet.filters,
            calculated_fields=worksheet.calculated_fields,
            field_mappings=field_mappings,
        )

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

    def _construct_field_mappings(self, view_mappings: Dict) -> Dict:
        """
        Construct structured field mappings from view mappings.

        Creates three JSON structures:
        - dimensions: {datasource_id: {"local_name": clean_name}}
        - measures: {datasource_id: {"local_name": clean_name, "aggregation": "sum"}}
        - calculated_fields: {datasource_id: {"local_name": clean_name}}

        Args:
            view_mappings: View mappings from generated views

        Returns:
            Dict with dimensions, measures, and calculated_fields mappings
        """
        field_mappings = {"dimensions": {}, "measures": {}, "calculated_fields": {}}

        for each_view in view_mappings:
            for table_name, table_value in each_view.items():
                # Process dimensions
                for each_dimension in table_value.get("dimensions", []):
                    datasource_id = each_dimension.get("datasource_id")
                    local_name = each_dimension.get("local_name")
                    clean_name = each_dimension.get("name")

                    if datasource_id:
                        if datasource_id not in field_mappings["dimensions"]:
                            field_mappings["dimensions"][datasource_id] = {}

                        field_mappings["dimensions"][datasource_id][local_name] = {
                            "clean_name": self._clean_name(clean_name)
                        }

                # Process measures
                for each_measure in table_value.get("measures", []):
                    datasource_id = each_measure.get("datasource_id")
                    local_name = each_measure.get("local_name")
                    clean_name = each_measure.get("name")
                    if datasource_id:
                        if datasource_id not in field_mappings["measures"]:
                            field_mappings["measures"][datasource_id] = {}
                        field_mappings["measures"][datasource_id][local_name] = {
                            "clean_name": self._clean_name(clean_name),
                            "aggregation": each_measure.get("aggregation"),
                        }

                # Process calculated fields
                for each_calculated_field in table_value.get("calculated_fields", []):
                    datasource_id = each_calculated_field.get("datasource_id")
                    local_name = each_calculated_field.get("local_name")
                    clean_name = each_calculated_field.get("name")
                    if datasource_id:
                        if datasource_id not in field_mappings["calculated_fields"]:
                            field_mappings["calculated_fields"][datasource_id] = {}
                        field_mappings["calculated_fields"][datasource_id][
                            local_name
                        ] = {"clean_name": self._clean_name(clean_name)}

        logger.debug(
            f"Constructed field mappings: {len(field_mappings['dimensions'])} dimension sources, "
            f"{len(field_mappings['measures'])} measure sources, "
            f"{len(field_mappings['calculated_fields'])} calculated field sources"
        )

        return field_mappings

    def _snake_case_filter(self, value: str) -> str:
        """Convert string to snake_case."""
        import re

        # Handle brackets and special characters
        value = re.sub(r"\[([^\]]+)\]", r"\1", value)  # Remove brackets
        value = re.sub(r"[^\w\s]", "_", value)  # Replace special chars with underscore
        value = re.sub(r"\s+", "_", value)  # Replace spaces with underscore
        value = re.sub(r"_+", "_", value)  # Replace multiple underscores with single
        value = value.strip(
            "_"
        ).lower()  # Remove leading/trailing underscores and lowercase

        return value

    def _clean_name(self, value: str) -> str:
        """Clean field names for LookML."""
        # Remove brackets and clean up
        clean_value = value.replace("[", "").replace("]", "")
        return self._snake_case_filter(clean_value)

    def _update_worksheet_fields_with_view_mappings(
        self, worksheet: WorksheetSchema, field_mappings: Dict
    ) -> List[str]:
        """Merged worksheet fields with view mappings"""
        dimensions = field_mappings.get("dimensions", {})
        measures = field_mappings.get("measures", {})
        calculated_fields = field_mappings.get("calculated_fields", {})
        for field in worksheet.fields:
            if field.role == "dimension":
                datasource_id = field.datasource_id
                local_name = field.original_name
                datasource_fields = dimensions.get(datasource_id, {})
                clean_name = None
                if datasource_fields:
                    clean_name = datasource_fields.get(local_name, {}).get("clean_name")
                    field.view_mapping_name = clean_name
                if not clean_name:
                    datasource_fields = calculated_fields.get(datasource_id, {})
                    if datasource_fields:
                        clean_name = datasource_fields.get(local_name, {}).get(
                            "clean_name"
                        )
                        field.view_mapping_name = clean_name

            elif field.role == "measure":
                datasource_id = field.datasource_id
                local_name = field.original_name
                datasource_fields = measures.get(datasource_id, {})
                clean_name = None
                aggregation = None
                if datasource_fields:
                    clean_name = datasource_fields.get(local_name, {}).get("clean_name")
                    aggregation = datasource_fields.get(local_name, {}).get(
                        "aggregation"
                    )
                if not clean_name:
                    datasource_fields = calculated_fields.get(datasource_id, {})
                    if datasource_fields:
                        clean_name = datasource_fields.get(local_name, {}).get(
                            "clean_name"
                        )
                        aggregation = datasource_fields.get(local_name, {}).get(
                            "aggregation"
                        )

                    # Convert both aggregations to comparable format
                field_agg = field.aggregation
                if hasattr(field_agg, "value"):
                    field_agg = field_agg.value.lower()  # enum to string
                else:
                    field_agg = str(field_agg).lower() if field_agg else ""

                view_agg = aggregation
                if hasattr(view_agg, "value"):
                    view_agg = view_agg.value.lower()  # enum to string
                else:
                    view_agg = str(view_agg).lower() if view_agg else ""

                if clean_name and (view_agg == field_agg or not view_agg):
                    field.view_mapping_name = clean_name
        return "Success"
