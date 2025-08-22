"""
WorksheetHandler for converting raw XML parser output to WorksheetSchema format.

Transforms the raw worksheet data from xml_parser_v2.extract_worksheets()
into validated WorksheetSchema objects.
"""

import logging
from typing import Dict, List, Optional, Any
from ..handlers.base_handler import BaseHandler
from ..models.worksheet_models import WorksheetSchema, ChartType
from ..converters.tableau_chart_rule_engine import TableauChartRuleEngine
from ..core.field_derivation_engine import FieldDerivationEngine

logger = logging.getLogger(__name__)


class WorksheetHandler(BaseHandler):
    """
    Handler for Tableau worksheet elements.

    Converts raw XML parser output into WorksheetSchema-compliant JSON.
    Handles field usage validation, visualization config processing, and confidence scoring.
    Enhanced with multi-tier chart type detection system.
    """

    def __init__(self, enable_yaml_detection: bool = True):
        """
        Initialize WorksheetHandler with YAML-based chart type detection.

        Args:
            enable_yaml_detection: Use TableauChartRuleEngine for rule-based detection
        """
        self.enable_yaml_detection = enable_yaml_detection
        if enable_yaml_detection:
            self.chart_detector = TableauChartRuleEngine()
        else:
            self.chart_detector = None

        # Initialize field derivation engine for Tableau instance processing
        self.field_derivation_engine = FieldDerivationEngine()

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
            # Debug output for CD detail
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
        clean_name = data.get("clean_name", self._clean_name(name))
        datasource_id = data["datasource_id"]

        if name == "bychannel2_chnl":
            print(f"Worksheet {name} has data: {data}")

        # Process fields
        fields = self._process_fields(data.get("fields", []))

        # Process visualization with YAML rule-based chart type detection
        visualization = self._process_visualization_with_yaml_rules(data, fields)

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

        # Identify derived fields from Tableau instances (time functions, aggregations)
        derived_fields = self._identify_derived_fields_from_visualization(
            visualization, datasource_id
        )
        logger.debug(
            f"WORKSHEET HANDLER: {name} has {len(derived_fields)} derived fields"
        )

        # Build WorksheetSchema data
        worksheet_data = {
            "name": name,
            "clean_name": clean_name,
            "title": data.get("title", ""),
            "datasource_id": datasource_id,
            "fields": fields,
            "calculated_fields": self._extract_calculated_fields(fields),
            "visualization": visualization,
            "filters": filters,
            "identified_measures": identified_measures,  # NEW: Measure data for Migration Engine
            "derived_fields": derived_fields,  # NEW: Derived field data for Migration Engine
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

        # NEW: Add styling data if extracted from XML parser with field-specific color mapping
        if "styling" in data:
            # Extract field-specific color mappings based on field encodings
            # Pass the datasource_id so we can match the correct datasource color mappings
            styling_data = self._extract_field_specific_styling(
                data["styling"], fields, datasource_id
            )
            worksheet_data["styling"] = styling_data
            logger.debug(
                f"Added field-specific styling data to worksheet {name}: {list(styling_data.keys())}"
            )

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
                "encodings": field.get("encodings", []),  # Add encodings list
                "derivation": field.get("derivation", "None"),
                "suggested_type": self._suggest_lookml_type(field),
                "drill_fields": [],
                "display_label": display_label,
                "datasource_id": field.get("datasource_id"),
            }

            processed_fields.append(field_ref)

        return processed_fields

    def _process_visualization_with_yaml_rules(
        self, worksheet_data: Dict, fields: List[Dict]
    ) -> Dict:
        """
        Process visualization with YAML rule-based chart type detection.

        Uses the TableauChartRuleEngine for configurable, rule-based detection.
        Falls back to basic processing if YAML detection is disabled.
        """
        raw_viz = worksheet_data.get("visualization", {})

        # Start with basic visualization processing
        viz_config = self._process_visualization_basic(raw_viz)

        # Apply YAML rule-based chart type detection
        if self.enable_yaml_detection and self.chart_detector:
            worksheet_name = worksheet_data.get("name", "unknown")
            logger.debug(f"Running YAML rule detection for worksheet: {worksheet_name}")

            # Debug YAML detection

            # Prepare data for YAML rule detection
            detection_input = {
                "name": worksheet_data.get("name", ""),
                "fields": fields,
                "visualization": viz_config,
                "datasource_id": worksheet_data.get("datasource_id"),
            }
            logger.debug(f"YAML detection input: {detection_input}")

            # Run YAML rule-based detection
            try:
                detection_result = self.chart_detector.detect_chart_type(
                    detection_input
                )
                logger.debug(f"YAML detection result: {detection_result}")

                logger.debug(
                    f"YAML detection result for {worksheet_name}: {detection_result}"
                )
            except Exception as e:
                logger.error(f"YAML detection failed for {worksheet_name}: {e}")
                # Fall back to basic detection
                return viz_config

            # Update visualization config with YAML rule results
            viz_config.update(
                {
                    "chart_type": detection_result["chart_type"],
                    "yaml_detection": {
                        "confidence": detection_result["confidence"],
                        "method": detection_result["method"],
                        "reasoning": detection_result.get("reasoning", ""),
                        "matched_rule": detection_result.get("matched_rule"),
                        "is_dual_axis": detection_result.get("is_dual_axis", False),
                        "looker_equivalent": detection_result.get("looker_equivalent"),
                        "pivot_required": detection_result.get("pivot_required", False),
                        "fields_sources": detection_result.get("fields_sources", []),
                        "pivot_field_source": detection_result.get(
                            "pivot_field_source", []
                        ),
                        "pivot_selection_logic": detection_result.get(
                            "pivot_selection_logic"
                        ),
                        "stacked_type": detection_result.get("stacked_type", False),
                    },
                }
            )
            # Set stacked flag only when the matched rule explicitly indicates stacking
            if detection_result.get("matched_rule") and detection_result.get(
                "stacked_type", False
            ):
                viz_config["stacked"] = True
            logger.info(
                f"YAML detection for '{worksheet_data.get('name')}': "
                f"{viz_config.get('chart_type')} (rule: {detection_result.get('matched_rule')})"
            )
        else:
            logger.debug(
                f"YAML detection disabled or detector missing: enabled={self.enable_yaml_detection}, detector={self.chart_detector is not None}"
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
        """Simple mapping for basic chart types: donut, pie, bar, table."""
        # Simple mapping for basic charts only
        if tableau_chart_type.lower() == "pie":
            return ChartType.PIE
        elif tableau_chart_type.lower() == "square":
            return ChartType.TEXT_TABLE  # Square = Table
        else:
            return ChartType.BAR  # Everything else = Bar

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

    def _identify_derived_fields_from_visualization(
        self, visualization: Dict, datasource_id: str
    ) -> List[Dict]:
        """
        Identify derived fields from visualization patterns like tdy:RPT_DT:ok and sum:sales:qk.

        Uses FieldDerivationEngine to detect time functions and aggregations from
        visualization axis and encoding patterns.

        Args:
            visualization: Visualization configuration with axis and encoding data
            datasource_id: Worksheet datasource identifier

        Returns:
            List of derived field data dicts for Migration Engine to process
        """
        derived_fields = []
        seen_patterns = set()  # Track unique patterns

        # Collect all patterns from visualization
        patterns = []

        # Get patterns from axes
        x_axis = visualization.get("x_axis", [])
        y_axis = visualization.get("y_axis", [])
        patterns.extend(x_axis)
        patterns.extend(y_axis)

        # Get patterns from encodings
        color = visualization.get("color", "")
        if color:
            patterns.append(color)

        size = visualization.get("size", "")
        if size:
            patterns.append(size)

        for pattern in patterns:
            if not isinstance(pattern, str) or pattern in seen_patterns:
                continue

            # Clean federated prefixes: [federated.xxx].[pattern] → pattern
            clean_pattern = self._clean_federated_pattern(pattern)
            if not clean_pattern:
                continue

            # Check if this looks like a derivable pattern
            if self._is_derivable_visualization_pattern(clean_pattern):
                # Derive field definition using the engine
                derived_field = self._derive_field_from_visualization_pattern(
                    clean_pattern, datasource_id
                )

                if derived_field:
                    derived_fields.append(derived_field)
                    seen_patterns.add(pattern)

                    logger.debug(
                        f"Derived field from visualization pattern: {pattern} → {derived_field['name']} ({derived_field['field_type']})"
                    )

        return derived_fields

    def _clean_federated_pattern(self, pattern: str) -> str:
        """
        Clean federated patterns to extract the actual field pattern.

        Examples:
        - [federated.xxx].[sum:sales:qk] → sum:sales:qk
        - tdy:RPT_DT:ok → tdy:RPT_DT:ok
        """
        if pattern.startswith("[federated.") and "].[" in pattern:
            # Extract pattern after ].[
            parts = pattern.split("].[")
            if len(parts) > 1:
                return parts[1].rstrip("]")

        return pattern

    def _is_derivable_visualization_pattern(self, pattern: str) -> bool:
        """Check if visualization pattern is derivable."""
        if not pattern or ":" not in pattern:
            return False

        parts = pattern.split(":")
        if len(parts) < 3:
            return False

        function = parts[0]
        field = parts[1]

        # Time functions
        if function in ["tdy", "thr", "tmn", "tqr", "tyr", "tmth", "twk"]:
            return True

        # Aggregation functions
        if function in ["sum", "avg", "cnt", "min", "max", "med"]:
            return True

        # Calculation references
        if field.startswith("Calculation_"):
            return True

        return False

    def _derive_field_from_visualization_pattern(
        self, pattern: str, datasource_id: str
    ) -> Optional[Dict]:
        """Derive field from visualization pattern."""
        parts = pattern.split(":")
        if len(parts) < 3:
            return None

        function = parts[0]
        field = parts[1]
        # qualifier = parts[2]  # Not currently used

        # Time functions
        time_functions = {
            "tdy": "day",
            "thr": "hour",
            "tmn": "minute",
            "tqr": "quarter",
            "tyr": "year",
            "tmth": "month",
            "twk": "week",
        }

        if function in time_functions:
            return {
                "name": field.lower(),
                "field_type": "dimension_group",
                "role": "dimension",
                "datatype": "datetime",
                "sql_column": field.upper(),
                "description": f"Time dimension group for {field.lower()}",
                "timeframes": [
                    "raw",
                    "time",
                    "date",
                    "week",
                    "month",
                    "quarter",
                    "year",
                ],
                "primary_timeframe": time_functions[function],
                "derivation": f"time_function:{time_functions[function]}",
                "tableau_instance": pattern,
                "original_tableau_instance": pattern,
                "is_derived": True,
                "source_type": "visualization_pattern",
                "table_name": self._extract_table_name(datasource_id),
            }

        # Aggregation functions
        agg_functions = {
            "sum": "sum",
            "avg": "average",
            "cnt": "count",
            "min": "min",
            "max": "max",
            "med": "median",
        }

        if function in agg_functions:
            return {
                "name": field.lower(),
                "field_type": "measure",
                "role": "measure",
                "datatype": "real",
                "sql_column": field.upper(),
                "description": f"{agg_functions[function].title()} of {field.lower()}",
                "aggregation": agg_functions[function],
                "lookml_type": agg_functions[function]
                if agg_functions[function] in ["sum", "count", "average", "min", "max"]
                else "sum",
                "derivation": f"aggregation:{agg_functions[function]}",
                "tableau_instance": pattern,
                "original_tableau_instance": pattern,
                "is_derived": True,
                "source_type": "visualization_pattern",
                "table_name": self._extract_table_name(datasource_id),
            }

        # Calculation references
        if field.startswith("Calculation_"):
            return {
                "name": field.lower(),
                "field_type": "dimension",  # Default, will be corrected by calc field data
                "role": "dimension",
                "datatype": "string",
                "description": f"Reference to calculated field {field.lower()}",
                "derivation": "calculation_reference",
                "tableau_instance": pattern,
                "original_tableau_instance": pattern,
                "is_derived": True,
                "is_calculation_reference": True,
                "source_type": "visualization_pattern",
                "table_name": self._extract_table_name(datasource_id),
            }

        return None

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
            # Primary check: if field has a formula, it's a calculated field
            if field.get("calculation", {}).get("original_formula"):
                calculated.append(field["name"])
            # Check for [Calculation_ pattern
            elif field.get("original_name", "").startswith("[Calculation_"):
                calculated.append(field["name"])
            # Check for other calculated field patterns like [Rolling, [Model Name, etc.
            elif field.get("original_name", "").startswith("[") and (
                "(copy)" in field.get("original_name", "")
                or "_copy_" in field.get("original_name", "")
            ):
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

        # YAML rule-based chart type detection confidence boost
        yaml_detection = visualization.get("yaml_detection", {})
        if yaml_detection:
            # Use the YAML detection confidence, weighted
            detection_confidence = yaml_detection.get("confidence", 0.5)
            detection_boost = (
                detection_confidence - 0.5
            ) * 0.3  # Scale to 0-0.15 boost
            confidence += detection_boost

            # Extra boost for dual-axis detection (high value)
            if yaml_detection.get("is_dual_axis", False):
                confidence += 0.05

            # Extra boost for rule matches (indicates good pattern matching)
            if yaml_detection.get("matched_rule"):
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

    def _extract_field_specific_styling(
        self, styling_data: Dict[str, Any], fields: List[Dict], datasource_id: str
    ) -> Dict[str, Any]:
        """
        Extract field-specific styling information, including datasource-specific color mappings.

        Args:
            styling_data: Raw styling data from TableauStyleExtractor
            fields: List of worksheet fields with datasource information
            datasource_id: The worksheet's datasource ID

        Returns:
            Enhanced styling data with field-specific color mappings
        """
        try:
            result_styling = styling_data.copy()

            # Get datasource-specific color mappings
            datasource_color_mappings = styling_data.get("field_color_mappings", {})
            if not datasource_color_mappings:
                logger.debug("No datasource color mappings found in styling data")
                return result_styling

            # Find fields with color encodings
            color_fields = [
                field for field in fields if "color" in field.get("encodings", [])
            ]

            if not color_fields:
                logger.debug(
                    "No fields with color encodings found - returning original styling"
                )
                return result_styling

            # Get the datasource ID from the worksheet data (not from individual fields)
            # We need to get this from the worksheet context, not from individual fields
            # For now, let's try to find any datasource that has color mappings
            if not datasource_color_mappings:
                logger.debug("No datasource color mappings found")
                return result_styling

            # Find the matching datasource color mappings using the worksheet's datasource_id
            matching_datasource_mappings = None
            for datasource_name, datasource_data in datasource_color_mappings.items():
                # Check if this datasource matches the worksheet's datasource
                if (
                    datasource_name == datasource_id
                    or datasource_name in datasource_id
                    or datasource_id in datasource_name
                ):
                    matching_datasource_mappings = datasource_data
                    logger.debug(
                        f"Found matching datasource color mappings for {datasource_name}"
                    )
                    break

            if not matching_datasource_mappings:
                logger.debug(
                    f"No matching datasource color mappings found for {datasource_id}"
                )
                return result_styling

            # For each color field, try to find its specific categorical color mapping
            for field in color_fields:
                field_name = field.get("name", "")
                original_name = field.get("original_name", "")
                tableau_instance = field.get("tableau_instance", "")

                # Try to match field with available color mappings from the matching datasource
                matching_mapping = self._find_matching_color_mapping(
                    field_name,
                    original_name,
                    tableau_instance,
                    matching_datasource_mappings["fields"],
                )

                if matching_mapping:
                    # Replace the generic color_mappings with field-specific ones
                    result_styling["color_mappings"] = matching_mapping
                    logger.debug(
                        f"Applied datasource-specific color mapping for field {field_name} from {datasource_id}: {list(matching_mapping.get('mappings', {}).keys())}"
                    )
                    break  # Use the first matching field's colors

            return result_styling

        except Exception as e:
            logger.warning(f"Failed to extract field-specific styling: {str(e)}")
            return styling_data

    def _find_matching_color_mapping(
        self,
        field_name: str,
        original_name: str,
        tableau_instance: str,
        field_color_mappings: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Find the best matching color mapping for a given field.

        Args:
            field_name: Clean field name (e.g., 'calculation_5910989867950081')
            original_name: Original field name (e.g., '[Calculation_5910989867950081]')
            tableau_instance: Tableau instance reference (e.g., '[none:Calculation_5910989867950081:nk]')
            field_color_mappings: Available field color mappings from datasource

        Returns:
            Matching color mapping dict or None if no match found
        """
        # Priority order for matching:
        # 1. Exact field name match
        # 2. Match in original name
        # 3. Match in tableau instance
        # 4. Partial substring match for calculated fields

        # Direct field name match
        if field_name in field_color_mappings:
            return field_color_mappings[field_name]

        # Check original name patterns
        for mapping_key, mapping_data in field_color_mappings.items():
            # Match calculation fields by ID
            if (
                "calculation_" in field_name.lower()
                and "calculation_" in mapping_key.lower()
            ):
                # Extract the numeric ID part
                field_id = (
                    field_name.split("calculation_")[-1]
                    if "calculation_" in field_name
                    else ""
                )
                mapping_id = (
                    mapping_key.split("calculation_")[-1]
                    if "calculation_" in mapping_key
                    else ""
                )
                if field_id and mapping_id and field_id == mapping_id:
                    return mapping_data

            # Match other field patterns (is_preorder, mkt, channel, etc.)
            if (
                mapping_key.lower() in field_name.lower()
                or field_name.lower() in mapping_key.lower()
            ):
                return mapping_data

            # Check if field name appears in tableau instance or original name
            if original_name and mapping_key in original_name:
                return mapping_data

            if tableau_instance and mapping_key in tableau_instance:
                return mapping_data

        logger.debug(f"No matching color mapping found for field {field_name}")
        return None
