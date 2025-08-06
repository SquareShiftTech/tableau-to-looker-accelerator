"""
WorksheetHandler for converting raw XML parser output to WorksheetSchema format.

Transforms the raw worksheet data from xml_parser_v2.extract_worksheets()
into validated WorksheetSchema objects.
"""

import logging
from typing import Dict, List, Optional, Any
from ..handlers.base_handler import BaseHandler
from ..models.worksheet_models import WorksheetSchema, ChartType
from ..converters.enhanced_chart_type_detector import EnhancedChartTypeDetector

logger = logging.getLogger(__name__)


class WorksheetHandler(BaseHandler):
    """
    Handler for Tableau worksheet elements.

    Converts raw XML parser output into WorksheetSchema-compliant JSON.
    Handles field usage validation, visualization config processing, and confidence scoring.
    Enhanced with multi-tier chart type detection system.
    """

    def __init__(self, enable_enhanced_detection: bool = True):
        """
        Initialize WorksheetHandler with optional enhanced chart type detection.

        Args:
            enable_enhanced_detection: Use EnhancedChartTypeDetector for better accuracy
        """
        self.enable_enhanced_detection = enable_enhanced_detection
        if enable_enhanced_detection:
            self.chart_detector = EnhancedChartTypeDetector()
        else:
            self.chart_detector = None

    def can_handle(self, data: Dict) -> float:
        """Check if data contains worksheet information."""
        if not isinstance(data, dict):
            return 0.0

        # Must have basic worksheet structure
        required_keys = ["name", "datasource_id", "fields", "visualization"]
        if not all(key in data for key in required_keys):
            return 0.0

        # Check field structure
        fields = data.get("fields", [])
        if not isinstance(fields, list):
            return 0.0

        # Check visualization structure
        viz = data.get("visualization", {})
        if not isinstance(viz, dict) or "chart_type" not in viz:
            return 0.0

        # Filter out worksheets that are likely text elements or placeholders
        # name = data.get("name", "").lower()

        # Check for text-only or placeholder worksheets
        if self._is_text_or_placeholder_worksheet(data, fields):
            return 0.0

        # High confidence if it has typical worksheet elements
        confidence = 0.8

        # Boost confidence for good field data
        if fields and all(
            isinstance(field, dict) and "name" in field for field in fields
        ):
            confidence += 0.1

        # Boost confidence for complete visualization data
        if "x_axis" in viz and "y_axis" in viz:
            confidence += 0.1

        return min(confidence, 1.0)

    def convert_to_json(self, data: Dict) -> Dict:
        """Convert raw worksheet data to WorksheetSchema-compliant JSON."""

        # Extract basic properties
        name = data["name"]
        if name == "CD detail":
            print(f"🔧 WORKSHEET DEBUG: Processing worksheet '{name}'")
        clean_name = data.get("clean_name", self._clean_name(name))
        datasource_id = data["datasource_id"]

        # Process fields
        fields = self._process_fields(data.get("fields", []))

        # Process visualization with enhanced chart type detection
        visualization = self._process_visualization_enhanced(data, fields)

        # Process filters and actions
        filters = data.get("filters", [])
        actions = data.get("actions", [])

        # Calculate confidence (enhanced by chart type detection)
        confidence = self._calculate_worksheet_confidence(data, fields, visualization)

        # Identify worksheet-specific measures from field aggregations
        identified_measures = self._identify_worksheet_measures(fields, datasource_id)
        logger.info(
            f"Identified {len(identified_measures)} worksheet-specific measures for {name}"
        )

        # Build WorksheetSchema data
        worksheet_data = {
            "name": name,
            "clean_name": clean_name,
            "datasource_id": datasource_id,
            "fields": fields,
            "calculated_fields": self._extract_calculated_fields(fields),
            "visualization": visualization,
            "filters": filters,
            "identified_measures": identified_measures,  # NEW: Measure data for Migration Engine
            "actions": actions,
            "dashboard_placements": [],  # Will be populated later by dashboard processing
            "suggested_explore_joins": self._suggest_joins(fields),
            "performance_hints": self._generate_performance_hints(
                fields, visualization
            ),
            "confidence": confidence,
            "parsing_errors": [],
            "custom_properties": {},
        }

        # Validate with Pydantic schema
        try:
            worksheet = WorksheetSchema(**worksheet_data)
            return worksheet.model_dump()
        except Exception as e:
            # If validation fails, return with lower confidence and error
            worksheet_data["confidence"] = 0.3
            worksheet_data["parsing_errors"] = [f"Schema validation failed: {str(e)}"]
            return worksheet_data

    def _process_fields(self, raw_fields: List[Dict]) -> List[Dict]:
        """Process raw field data into FieldReference format."""
        processed_fields = []

        for field in raw_fields:
            if not isinstance(field, dict) or "name" not in field:
                continue

            # Create display label from caption or original name
            caption = field.get("caption")
            if caption:
                display_label = caption
            else:
                # Fallback: clean the original name (remove brackets)
                original = field.get("original_name", f"[{field['name'].title()}]")
                display_label = original.strip("[]").replace("_", " ").title()

            field_ref = {
                "name": field["name"],
                "original_name": field.get(
                    "original_name", f"[{field['name'].title()}]"
                ),
                "tableau_instance": field.get("tableau_instance", ""),
                "datatype": field.get("datatype", "string"),
                "role": field.get("role", "dimension"),
                "aggregation": field.get("aggregation"),
                "shelf": field.get("shelf", "unknown"),
                "derivation": field.get("derivation", "None"),
                "suggested_type": self._suggest_lookml_type(field),
                "drill_fields": [],
                "display_label": display_label,
            }

            processed_fields.append(field_ref)

        return processed_fields

    def _process_visualization_enhanced(
        self, worksheet_data: Dict, fields: List[Dict]
    ) -> Dict:
        """
        Process visualization with enhanced chart type detection.

        Uses the EnhancedChartTypeDetector for better accuracy, falls back to
        basic processing if enhanced detection is disabled.
        """
        raw_viz = worksheet_data.get("visualization", {})

        # Start with basic visualization processing
        viz_config = self._process_visualization_basic(raw_viz)

        # Enhance with advanced chart type detection
        if self.enable_enhanced_detection and self.chart_detector:
            logger.debug(
                f"Running enhanced detection for worksheet: {worksheet_data.get('name', 'unknown')}"
            )
            # Prepare data for enhanced detection
            detection_data = {
                "name": worksheet_data.get("name", ""),
                "fields": fields,
                "visualization": viz_config,
            }
            logger.debug(f"Detection input data: {detection_data}")

            # Run enhanced detection
            detection_result = self.chart_detector.detect_chart_type(detection_data)
            logger.debug(f"Enhanced detection result: {detection_result}")

            # Update visualization config with enhanced results
            viz_config.update(
                {
                    "chart_type": detection_result["chart_type"],
                    "enhanced_detection": {
                        "confidence": detection_result["confidence"],
                        "method": detection_result["method"].value
                        if hasattr(detection_result["method"], "value")
                        else str(detection_result["method"]),
                        "reasoning": detection_result.get("reasoning", ""),
                        "is_dual_axis": detection_result.get("is_dual_axis", False),
                        "primary_type": detection_result.get("primary_type"),
                        "secondary_type": detection_result.get("secondary_type"),
                    },
                }
            )
            logger.debug(
                f"Updated visualization config chart_type: {viz_config.get('chart_type')}"
            )
        else:
            logger.debug(
                f"Enhanced detection disabled or detector missing: enabled={self.enable_enhanced_detection}, detector={self.chart_detector is not None}"
            )

        return viz_config

    def _process_visualization_basic(self, raw_viz: Dict) -> Dict:
        """Process raw visualization data into VisualizationConfig format (basic detection)."""

        # Map chart type to enum
        chart_type_str = raw_viz.get("chart_type", "").lower()
        chart_type = self._map_chart_type(chart_type_str)

        # Extract axis information
        x_axis = raw_viz.get("x_axis", [])
        y_axis = raw_viz.get("y_axis", [])

        # Handle string fields that should be lists
        if isinstance(x_axis, str):
            x_axis = [x_axis]
        if isinstance(y_axis, str):
            y_axis = [y_axis]

        viz_config = {
            "chart_type": chart_type.value,
            "x_axis": x_axis,
            "y_axis": y_axis,
            "color": raw_viz.get("color"),
            "size": raw_viz.get("size"),
            "detail": raw_viz.get("detail", []),
            "tooltip": raw_viz.get("tooltip", []),
            "is_dual_axis": raw_viz.get("is_dual_axis", False),
            "secondary_chart_type": None,  # TODO: Extract from dual axis config
            "stacked": False,  # TODO: Extract from mark properties
            "show_labels": raw_viz.get("show_labels", False),
            "show_totals": raw_viz.get("show_totals", False),
            "sort_fields": raw_viz.get("sorts", []),
            "raw_config": raw_viz,  # Preserve original for future use
        }

        return viz_config

    def _map_chart_type(self, tableau_chart_type: str) -> ChartType:
        """Map Tableau chart type to ChartType enum."""
        mapping = {
            "bar": ChartType.BAR,
            "line": ChartType.LINE,
            "area": ChartType.AREA,
            "pie": ChartType.PIE,
            "circle": ChartType.SCATTER,
            "scatter": ChartType.SCATTER,
            "map": ChartType.MAP,
            "text": ChartType.TEXT,
            "gantt": ChartType.GANTT,
            "square": ChartType.HEATMAP,
            "heatmap": ChartType.HEATMAP,
        }

        return mapping.get(tableau_chart_type.lower(), ChartType.UNKNOWN)

    def _suggest_lookml_type(self, field: Dict) -> Optional[str]:
        """Suggest appropriate LookML field type."""
        role = field.get("role", "").lower()
        datatype = field.get("datatype", "").lower()
        aggregation = (field.get("aggregation") or "").lower()

        if role == "dimension":
            if datatype in ["date", "datetime"]:
                return "dimension_group"
            elif datatype == "real":
                return "dimension"  # Numeric dimension
            else:
                return "dimension"
        elif role == "measure":
            if aggregation in ["sum", "avg", "count", "min", "max"]:
                return "measure"
            else:
                return "measure"

        return None

    def _identify_worksheet_measures(
        self, fields: List[Dict], datasource_id: str
    ) -> List[Dict]:
        """
        Identify worksheet-specific measures from field aggregations.

        Returns standardized measure data that Migration Engine can route through MeasureHandler.
        No coupling to MeasureHandler - just returns data.

        Args:
            fields: Processed worksheet fields with aggregation info
            datasource_id: Worksheet datasource identifier

        Returns:
            List of measure data dicts for Migration Engine to process
        """
        identified_measures = []
        seen_measures = set()  # Track unique field-aggregation combinations

        for field in fields:
            # Only process measure fields with non-standard aggregations
            if (
                field.get("role") == "measure"
                and field.get("aggregation")
                and field.get("aggregation").lower() not in ["sum", "none"]
            ):
                field_name = field.get("name")
                aggregation = field.get("aggregation", "").lower()

                # Check for duplicates: same field + aggregation combination
                measure_key = f"{field_name}_{aggregation}"
                if measure_key in seen_measures:
                    continue
                seen_measures.add(measure_key)

                # Create standardized measure data (no handler coupling)
                measure_data = {
                    "name": field_name,
                    "raw_name": field.get("original_name", f"[{field_name.title()}]"),
                    "role": "measure",
                    "aggregation": aggregation,
                    "datatype": field.get("datatype", "real"),
                    "table_name": self._extract_table_name(datasource_id),
                    "label": field.get(
                        "display_label", field_name.replace("_", " ").title()
                    ),
                    "caption": f"Worksheet-specific {aggregation.upper()} aggregation",
                    "source_type": "worksheet_field",  # Track source
                    "tableau_instance": field.get("tableau_instance", ""),
                }

                identified_measures.append(measure_data)
                logger.debug(
                    f"Identified worksheet measure: {field_name} ({aggregation.upper()})"
                )

        return identified_measures

    def _extract_table_name(self, datasource_id: str) -> str:
        """Extract table name from datasource ID for worksheet measures."""
        if not datasource_id:
            return "Orders"  # Default fallback

        # Extract from federated datasource pattern
        if "federated" in datasource_id:
            return "Orders"  # Most common case in our samples

        return datasource_id.split(".")[-1] if "." in datasource_id else datasource_id

    def _extract_calculated_fields(self, fields: List[Dict]) -> List[str]:
        """Extract names of calculated fields from field list."""
        calculated = []

        for field in fields:
            # Check if field looks like a calculated field
            original_name = field.get("original_name", "")
            if original_name.startswith("[Calculation_"):
                calculated.append(field["name"])
            # Check tableau instance for calculated field patterns
            elif "calculation" in field.get("tableau_instance", "").lower():
                calculated.append(field["name"])

        return calculated

    def _suggest_joins(self, fields: List[Dict]) -> List[str]:
        """Suggest potential join relationships based on field usage."""
        joins = []

        # Look for fields that might indicate joins
        for field in fields:
            field_name = field.get("name", "").lower()
            if any(keyword in field_name for keyword in ["id", "key", "code"]):
                # This might be a foreign key
                table_name = (
                    field_name.replace("_id", "")
                    .replace("_key", "")
                    .replace("_code", "")
                )
                if (
                    table_name != field_name
                ):  # Only if we extracted something meaningful
                    joins.append(table_name)

        return list(set(joins))  # Remove duplicates

    def _generate_performance_hints(
        self, fields: List[Dict], visualization: Dict
    ) -> Dict[str, Any]:
        """Generate performance optimization hints."""
        hints = {}

        # Count high-cardinality dimensions
        high_cardinality_dims = []
        for field in fields:
            if field.get("role") == "dimension" and field.get("datatype") == "string":
                high_cardinality_dims.append(field["name"])

        if high_cardinality_dims:
            hints["high_cardinality_dimensions"] = high_cardinality_dims
            hints["suggested_indexes"] = high_cardinality_dims

        # Check for complex aggregations
        complex_measures = []
        for field in fields:
            if field.get("role") == "measure" and field.get("aggregation") in [
                "avg",
                "count_distinct",
            ]:
                complex_measures.append(field["name"])

        if complex_measures:
            hints["complex_aggregations"] = complex_measures

        # Chart-specific hints
        chart_type = visualization.get("chart_type")
        if chart_type == "scatter" and len(fields) > 10:
            hints["chart_optimization"] = (
                "Consider limiting dimensions for scatter plots"
            )

        return hints

    def _calculate_worksheet_confidence(
        self, data: Dict, fields: List[Dict], visualization: Dict
    ) -> float:
        """Calculate confidence score for worksheet processing (enhanced)."""
        confidence = 0.7  # Base confidence

        # Boost for complete field data
        if fields and all("name" in field and "role" in field for field in fields):
            confidence += 0.1

        # Enhanced chart type detection confidence boost
        enhanced_detection = visualization.get("enhanced_detection", {})
        if enhanced_detection:
            # Use the enhanced detection confidence, weighted
            detection_confidence = enhanced_detection.get("confidence", 0.5)
            detection_boost = (
                detection_confidence - 0.5
            ) * 0.3  # Scale to 0-0.15 boost
            confidence += detection_boost

            # Extra boost for dual-axis detection (high value)
            if enhanced_detection.get("is_dual_axis", False):
                confidence += 0.05
        else:
            # Fallback to basic chart type check
            if visualization.get("chart_type") != ChartType.UNKNOWN.value:
                confidence += 0.1

        # Boost for axis information
        if visualization.get("x_axis") and visualization.get("y_axis"):
            confidence += 0.1

        # Penalty for missing key data
        if not data.get("datasource_id"):
            confidence -= 0.2

        if not fields:
            confidence -= 0.3

        return max(0.0, min(1.0, confidence))

    def _is_text_or_placeholder_worksheet(self, data: Dict, fields: List[Dict]) -> bool:
        """
        Check if a worksheet is likely a text element or placeholder rather than a data visualization.

        Args:
            data: Raw worksheet data
            fields: Processed field list

        Returns:
            bool: True if worksheet should be filtered out
        """
        name = data.get("name", "").lower()

        # Check for common text/placeholder names
        text_indicators = [
            "notice",
            "text",
            "title",
            "header",
            "footer",
            "label",
            "placeholder",
            "blank",
            "spacer",
            "divider",
            "instruction",
            "filter",
            "refresh",
        ]

        name_matches_indicator = any(indicator in name for indicator in text_indicators)

        # Check if worksheet has no meaningful visualization data
        viz = data.get("visualization", {})
        has_no_viz_data = (
            not viz.get("x_axis") and not viz.get("y_axis") and not viz.get("color")
        )

        # If name suggests it's a text element AND it has no visualization data, filter it
        if name_matches_indicator and has_no_viz_data:
            return True

        # Check if all fields are calculated fields with empty formulas
        if fields and all(self._is_empty_calculated_field(field) for field in fields):
            return True

        # Additional check for worksheets with text-like names but some fields
        if name_matches_indicator:
            # Check if the fields are meaningful or just placeholders
            meaningful_fields = self._count_meaningful_fields(fields)
            if meaningful_fields <= 1:  # Only one or no meaningful fields
                return True

        return False

    def _has_only_empty_or_text_fields(self, fields: List[Dict]) -> bool:
        """Check if fields are only empty calculated fields or static text."""
        if not fields:
            return True

        meaningful_fields = 0
        for field in fields:
            # Skip calculated fields with empty formulas
            if self._is_empty_calculated_field(field):
                continue

            # Skip fields that are just static text or constants
            original_name = field.get("original_name", "")
            if original_name.startswith("[Calculation_") and not field.get(
                "aggregation"
            ):
                continue

            meaningful_fields += 1

        return meaningful_fields == 0

    def _count_meaningful_fields(self, fields: List[Dict]) -> int:
        """Count meaningful fields (non-placeholder, non-empty calculated fields)."""
        meaningful_count = 0

        for field in fields:
            # Skip empty calculated fields
            if self._is_empty_calculated_field(field):
                continue

            # Count fields that have meaningful data
            original_name = field.get("original_name", "")
            aggregation = field.get("aggregation")
            role = field.get("role", "")

            # Count non-calculated fields as meaningful
            if not original_name.startswith("[Calculation_"):
                meaningful_count += 1
            # Count calculated fields with aggregation or measure role as meaningful
            elif aggregation or role == "measure":
                meaningful_count += 1
            # Count calculated fields with non-empty role as potentially meaningful
            elif role and role != "dimension":
                meaningful_count += 1

        return meaningful_count

    def _is_empty_calculated_field(self, field: Dict) -> bool:
        """Check if a field is a calculated field with empty or trivial formula."""
        original_name = field.get("original_name", "")

        # Check if it's a calculated field
        if not original_name.startswith("[Calculation_"):
            return False

        # Check if it has no meaningful aggregation or role
        role = field.get("role", "")
        aggregation = field.get("aggregation")

        # Fields with only empty string formulas are likely placeholders
        if role == "dimension" and not aggregation:
            return True

        return False

    def _clean_name(self, name: str) -> str:
        """Convert name to LookML-safe format."""
        import re

        # Convert to snake_case and remove special characters
        clean = re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())
        clean = re.sub(r"_+", "_", clean)  # Remove multiple underscores
        return clean.strip("_")
