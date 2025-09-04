"""
Tableau Chart Rule Engine - YAML-based chart type detection

Replaces the hardcoded EnhancedChartTypeDetector with a flexible YAML rule system.
Loads detection rules from chart_detection.yaml and matches them against worksheet data.
"""

import logging
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from enum import Enum


class ChartType(str, Enum):
    """Chart types supported by the rule engine."""

    BAR = "bar"
    COLUMN = "column"
    LINE = "line"
    AREA = "area"
    PIE = "pie"
    DONUT = "donut"
    SCATTER = "scatter"
    TEXT_TABLE = "text_table"
    HISTOGRAM = "histogram"
    BOX_PLOT = "box_plot"
    TREEMAP = "treemap"
    SYMBOL_MAP = "symbol_map"
    FILLED_MAP = "filled_map"
    UNKNOWN = "unknown"


class DetectionMethod(str, Enum):
    """Detection methods for tracking rule application."""

    TABLEAU_MARK_DIRECT = "tableau_mark_direct"
    DUAL_AXIS_DETECTION = "dual_axis_detection"
    FIELD_ANALYSIS = "field_analysis"
    WORKSHEET_NAME_EXACT = "worksheet_name_exact"
    WORKSHEET_NAME_PATTERN = "worksheet_name_pattern"
    AUTOMATIC_INFERENCE = "automatic_inference"
    FALLBACK_DEFAULT = "fallback_default"


class TableauChartRuleEngine:
    """
    YAML-based chart type detection engine for Tableau worksheets.

    Loads rules from chart_detection.yaml and applies them in priority order
    to determine the most appropriate chart type for each worksheet.
    """

    def __init__(self, yaml_config_path: Optional[str] = None):
        """
        Initialize the rule engine with YAML configuration.

        Args:
            yaml_config_path: Path to chart_detection.yaml file.
                             If None, looks for it in the project root.
        """
        self.logger = logging.getLogger(__name__)

        # Load YAML configuration
        if yaml_config_path is None:
            # Default path in config directory
            yaml_config_path = (
                Path(__file__).parent.parent / "config" / "chart_detection.yaml"
            )

        self.config_path = Path(yaml_config_path)
        self.rules = self._load_yaml_rules()

        # Chart type mappings
        self.chart_type_mappings = self._build_chart_type_mappings()

        self.logger.info(
            f"TableauChartRuleEngine initialized with {len(self.rules)} rule groups"
        )

    def _load_yaml_rules(self) -> Dict[str, Any]:
        """Load and parse the YAML rules configuration."""
        try:
            if not self.config_path.exists():
                self.logger.error(f"YAML config file not found: {self.config_path}")
                return {
                    "basic_chart_detection": {},
                    "fallback": self._get_default_fallback(),
                }

            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            if not config:
                self.logger.warning("Empty YAML config, using defaults")
                return {
                    "basic_chart_detection": {},
                    "fallback": self._get_default_fallback(),
                }

            self.logger.info(f"Loaded YAML rules from {self.config_path}")
            return config

        except Exception as e:
            self.logger.error(f"Failed to load YAML config: {e}")
            return {
                "basic_chart_detection": {},
                "fallback": self._get_default_fallback(),
            }

    def _get_default_fallback(self) -> Dict[str, Any]:
        """Get default fallback configuration."""
        return {
            "default_chart_type": "bar",
            "default_confidence": 0.40,
            "default_method": "fallback_default",
            "default_reason": "No matching rules, using bar chart fallback",
        }

    def _build_chart_type_mappings(self) -> Dict[str, ChartType]:
        """Build chart type mappings from YAML config."""
        mappings = {}

        # Get chart types from basic_chart_detection section
        chart_types = self.rules.get("basic_chart_detection", {})
        for chart_name in chart_types.keys():
            try:
                # Map YAML chart names to ChartType enum
                if chart_name == "column_chart":
                    mappings[chart_name] = ChartType.COLUMN
                elif chart_name == "bar_chart":
                    mappings[chart_name] = ChartType.BAR
                elif chart_name == "line_chart":
                    mappings[chart_name] = ChartType.LINE
                elif chart_name == "area_chart":
                    mappings[chart_name] = ChartType.AREA
                elif chart_name == "pie_chart":
                    mappings[chart_name] = ChartType.PIE
                elif chart_name == "donut_chart":
                    mappings[chart_name] = ChartType.DONUT
                elif chart_name == "scatter_plot":
                    mappings[chart_name] = ChartType.SCATTER
                elif chart_name == "text_table":
                    mappings[chart_name] = ChartType.TEXT_TABLE
                elif chart_name == "table_chart":
                    mappings[chart_name] = ChartType.TEXT_TABLE
                elif chart_name == "histogram":
                    mappings[chart_name] = ChartType.HISTOGRAM
                elif chart_name == "box_plot":
                    mappings[chart_name] = ChartType.BOX_PLOT
                elif chart_name == "treemap":
                    mappings[chart_name] = ChartType.TREEMAP
                elif chart_name == "symbol_map":
                    mappings[chart_name] = ChartType.SYMBOL_MAP
                elif chart_name == "filled_map":
                    mappings[chart_name] = ChartType.FILLED_MAP
                else:
                    mappings[chart_name] = ChartType.UNKNOWN

            except Exception as e:
                self.logger.warning(f"Failed to map chart type {chart_name}: {e}")
                mappings[chart_name] = ChartType.UNKNOWN

        return mappings

    def detect_chart_type(self, worksheet_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect chart type for a worksheet using YAML rules.

        Args:
            worksheet_data: Dictionary containing worksheet information from xml_parser_v2

        Returns:
            Dictionary with chart_type, confidence, method, reasoning, and metadata
        """
        worksheet_name = worksheet_data.get("name", "unknown")

        self.logger.debug(f"Detecting chart type for worksheet: '{worksheet_name}'")

        # Extract visualization config from worksheet data
        viz_config = worksheet_data.get("visualization", {})
        fields = worksheet_data.get("fields", [])

        # Prepare detection context
        detection_context = self._build_detection_context(
            worksheet_data, viz_config, fields
        )

        # Apply rules in priority order
        detection_result = self._apply_rules(detection_context, worksheet_name)

        self.logger.info(
            f"Chart detection result for '{worksheet_name}': "
            f"{detection_result['chart_type']} (confidence: {detection_result['confidence']:.2f})"
        )

        return detection_result

    def _build_detection_context(
        self, worksheet_data: Dict, viz_config: Dict, fields: List[Dict]
    ) -> Dict[str, Any]:
        """Build detection context from worksheet data for rule matching."""

        # Extract Tableau mark type from raw config (preserves original XML value)
        raw_config = viz_config.get("raw_config", {})
        # raw_mark_type = raw_config.get("chart_type") or viz_config.get(
        #     "chart_type", "automatic"
        # )
        # mark_type = str(
        #     raw_mark_type
        # ).title()  # Convert to "Square", "Bar", "Pie", etc.
        # Prefer the extracted chart type if available
        chart_type_extracted = raw_config.get("chart_type_extracted")

        if chart_type_extracted:
            mark_type = str(chart_type_extracted).title()
        else:
            raw_mark_type = raw_config.get("chart_type") or viz_config.get("chart_type", "automatic")
            # If it's a dict of marks, fallback to first value
            if isinstance(raw_mark_type, dict):
                raw_mark_type = list(raw_mark_type.values())[0]
            mark_type = str(raw_mark_type).title()

        # Check for dual axis
        has_dual_axis = viz_config.get("is_dual_axis", False)

        # Extract axis information
        x_axis_fields = viz_config.get("x_axis", [])
        y_axis_fields = viz_config.get("y_axis", [])

        # Extract encoding information
        color_field = viz_config.get("color")
        size_field = viz_config.get("size")

        # Classify x and y encodings
        x_encoding = self._classify_axis_encoding(x_axis_fields, fields)
        y_encoding = self._classify_axis_encoding(y_axis_fields, fields)

        # Determine orientation (vertical = column, horizontal = bar)
        orientation = self._determine_orientation(x_encoding, y_encoding)

        # Analyze field types
        field_analysis = self._analyze_fields(fields)

        # Count fields by type and shelf
        dimensions_on_x = len(
            [
                f
                for f in fields
                if f.get("shelf") == "columns" and f.get("role") == "dimension"
            ]
        )
        measures_on_y = len(
            [
                f
                for f in fields
                if f.get("shelf") == "rows" and f.get("role") == "measure"
            ]
        )

        # Check for text marks (either from fields or from encodings)
        has_text_marks = any(f.get("shelf") == "text" for f in fields)

        # Also check raw encodings for text columns (for Square/table charts)
        text_columns = []
        if raw_config:
            # First check direct encodings
            encodings = raw_config.get("encodings", {})
            text_columns = encodings.get("text_columns", [])

            # If no text columns found, check nested raw_config (nested structure)
            if not text_columns and "raw_config" in raw_config:
                nested_config = raw_config.get("raw_config", {})
                nested_encodings = nested_config.get("encodings", {})
                text_columns = nested_encodings.get("text_columns", [])

            if text_columns:
                has_text_marks = True

        # Check if text encoding has measure (for table charts)
        text_encoding_has_measure = False
        if text_columns:
            # Check if any text column contains measure indicators like :qk (quantitative key)
            text_encoding_has_measure = any(
                ":qk" in col or "sum:" in col.lower() or "avg:" in col.lower()
                for col in text_columns
            )

        # Note: inner_radius was removed from YAML rules since pie + dual_axis is sufficient for donut detection

        # Build context dictionary
        context = {
            # Basic worksheet info
            "worksheet_name": worksheet_data.get("name", ""),
            "worksheet_name_lower": worksheet_data.get("name", "").lower(),
            # Tableau mark information
            "mark_type": mark_type,
            "has_dual_axis": has_dual_axis,
            # Axis and shelf information
            "x_axis_fields": x_axis_fields,
            "y_axis_fields": y_axis_fields,
            "dimensions_on_x_axis": dimensions_on_x,
            "measures_on_y_axis": measures_on_y,
            # Encoding information
            "x_encoding": x_encoding,
            "y_encoding": y_encoding,
            "orientation": orientation,
            "color_encoding": self._classify_field_encoding(color_field, fields),
            "size_encoding": self._classify_field_encoding(size_field, fields),
            "has_color_encoding": bool(color_field),
            "has_size_encoding": bool(size_field),
            "has_no_color_size_encoding": not bool(color_field)
            and not bool(size_field),
            "has_label_encoding": any(f.get("shelf") == "label" for f in fields),
            "has_continuous_color_scale": False,  # TODO: Extract from color field analysis
            "has_latitude_longitude_encoding": any(
                "lat" in f.get("name", "").lower() or "lng" in f.get("name", "").lower()
                for f in fields
            ),
            "has_hierarchical_layout": False,  # TODO: Extract from mark properties
            "has_angle_encoding": any(f.get("shelf") == "angle" for f in fields),
            "has_multiple_measures": len(
                [f for f in fields if f.get("role") == "measure"]
            )
            > 1,
            "has_mark_stacking": False,  # TODO: Extract from mark properties
            "has_binned_fields": any(
                "bin" in f.get("name", "").lower() for f in fields
            ),
            # Text and table indicators
            "has_text_marks": has_text_marks,
            "text_encoding_has_measure": text_encoding_has_measure,
            "columns_shelf_count": len(x_axis_fields),
            "rows_shelf_count": len(y_axis_fields),
            "rows_shelf_has_string": any(
                f.get("datatype") == "string"
                for f in fields
                if f.get("shelf") == "rows"
            ),
            # Field analysis
            "total_dimensions": field_analysis["total_dimensions"],
            "total_measures": field_analysis["total_measures"],
            "has_date_fields": field_analysis["has_date_fields"],
            "has_geographic_fields": field_analysis["has_geographic_fields"],
            # Raw data for debugging
            "raw_viz_config": viz_config,
            "field_count": len(fields),
        }
        if worksheet_data.get('name', '') == "Device TR Ranking":
            print(f"Detection context for worksheet '{worksheet_data.get('name', '')}':\n{context}\n")

        self.logger.debug(f"Detection context: {context}")
        return context

    def _analyze_fields(self, fields: List[Dict]) -> Dict[str, Any]:
        """Analyze fields to extract type information."""
        analysis = {
            "total_dimensions": 0,
            "total_measures": 0,
            "has_date_fields": False,
            "has_geographic_fields": False,
            "categorical_fields": [],
            "continuous_fields": [],
        }

        for field in fields:
            role = field.get("role", "")
            datatype = field.get("datatype", "")
            field_name = field.get("name", "").lower()

            # Count by role
            if role == "dimension":
                analysis["total_dimensions"] += 1
                analysis["categorical_fields"].append(field)
            elif role == "measure":
                analysis["total_measures"] += 1
                analysis["continuous_fields"].append(field)

            # Check for date fields
            if datatype in ["date", "datetime"] or "date" in field_name:
                analysis["has_date_fields"] = True

            # Check for geographic fields (basic heuristic)
            if any(
                geo_term in field_name
                for geo_term in [
                    "lat",
                    "lng",
                    "longitude",
                    "latitude",
                    "geo",
                    "location",
                ]
            ):
                analysis["has_geographic_fields"] = True

        return analysis

    def _classify_field_encoding(
        self, field_name: Optional[str], fields: List[Dict]
    ) -> Optional[str]:
        """Classify a field encoding as categorical, measure, or temporal."""
        if not field_name:
            return None

        # Find the field in the fields list
        field_info = None
        for field in fields:
            if (
                field.get("name") == field_name
                or field.get("original_name") == field_name
            ):
                field_info = field
                break

        if not field_info:
            return None

        # Classify based on role and datatype
        role = field_info.get("role", "")
        datatype = field_info.get("datatype", "")

        if role == "measure":
            return "measure"
        elif datatype in ["date", "datetime"]:
            return "temporal"
        elif role == "dimension":
            return "categorical"

        return "categorical"  # Default

    def _classify_axis_encoding(
        self, axis_fields: List[str], fields: List[Dict]
    ) -> str:
        """Classify axis encoding as categorical, measure, or temporal."""
        if not axis_fields:
            return "none"

        # Check the datatype/role of fields on this axis
        encodings = []
        for axis_field in axis_fields:
            # Find matching field by checking tableau instance or name
            for field in fields:
                tableau_instance = field.get("tableau_instance", "")
                field_name = field.get("name", "")

                if axis_field in tableau_instance or axis_field in field_name:
                    role = field.get("role", "")
                    datatype = field.get("datatype", "")

                    if role == "measure":
                        encodings.append("measure")
                    elif (
                        datatype in ["date", "datetime"] or "date" in field_name.lower()
                    ):
                        encodings.append("temporal")
                    elif datatype == "string" or role == "dimension":
                        encodings.append("categorical")
                    break

        # Return the most specific encoding found
        if "measure" in encodings:
            return "measure"
        elif "temporal" in encodings:
            return "temporal"
        elif "categorical" in encodings:
            return "categorical"

        return "categorical"  # Default fallback

    def _determine_orientation(self, x_encoding: str, y_encoding: str) -> str:
        """Determine chart orientation based on axis encodings."""
        # Standard convention:
        # Vertical (column): categorical/temporal on x, measure on y
        # Horizontal (bar): measure on x, categorical/temporal on y

        if x_encoding in ["categorical", "temporal"] and y_encoding == "measure":
            return "vertical"
        elif x_encoding == "measure" and y_encoding in ["categorical", "temporal"]:
            return "horizontal"
        elif x_encoding == "measure" and y_encoding == "none":
            return "horizontal"
        return "vertical"  # Default to vertical (column chart)

    def _apply_rules(
        self, context: Dict[str, Any], worksheet_name: str
    ) -> Dict[str, Any]:
        """Apply YAML rules in priority order to determine chart type."""

        basic_rules = self.rules.get("basic_chart_detection", {})

        # Sort rules by confidence (highest first) as a proxy for priority
        sorted_rules = sorted(
            basic_rules.items(), key=lambda x: x[1].get("confidence", 0), reverse=True
        )

        for rule_name, rule_config in sorted_rules:
            self.logger.debug(f"Evaluating rule: {rule_name}")

            conditions = rule_config.get("conditions", [])
            if self._evaluate_conditions(conditions, context):
                chart_type = self.chart_type_mappings.get(rule_name, ChartType.UNKNOWN)
                confidence = (
                    rule_config.get("confidence", 50) / 100.0
                )  # Convert to 0-1 scale

                result = {
                    "chart_type": chart_type.value,
                    "confidence": confidence,
                    "method": DetectionMethod.TABLEAU_MARK_DIRECT,
                    "reasoning": f"Matched YAML rule: {rule_name}",
                    "matched_rule": rule_name,
                    "looker_equivalent": rule_config.get(
                        "looker_equivalent", "looker_column"
                    ),
                    "pivot_required": rule_config.get("pivot_required", False),
                    "fields_sources": rule_config.get("fields_sources", []),
                    "pivot_field_source": rule_config.get("pivot_field_source", []),
                    "pivot_selection_logic": rule_config.get("pivot_selection_logic"),
                    "is_dual_axis": context.get("has_dual_axis", False),
                    "stacked_type": rule_config.get("stacked_type", False),
                }

                self.logger.debug(
                    f"Rule '{rule_name}' matched with confidence {confidence}"
                )
                return result

        # No rules matched, use fallback
        fallback = self.rules.get("fallback", self._get_default_fallback())

        result = {
            "chart_type": fallback.get("default_chart_type", "bar"),
            "confidence": fallback.get("default_confidence", 0.40),
            "method": DetectionMethod.FALLBACK_DEFAULT,
            "reasoning": fallback.get("default_reason", "No matching rules found"),
            "matched_rule": None,
            "looker_equivalent": "looker_column",
            "pivot_required": False,
            "fields_sources": [],
            "pivot_field_source": [],
            "is_dual_axis": context.get("has_dual_axis", False),
        }

        self.logger.debug(f"No rules matched, using fallback: {result['chart_type']}")
        return result

    def _evaluate_conditions(
        self, conditions: List[Dict[str, Any]], context: Dict[str, Any]
    ) -> bool:
        """Evaluate rule conditions against detection context."""
        if not conditions:
            return False

        for condition in conditions:
            for condition_key, expected_value in condition.items():
                if not self._evaluate_single_condition(
                    condition_key, expected_value, context
                ):
                    return False

        return True

    def _evaluate_single_condition(
        self, condition_key: str, expected_value: Any, context: Dict[str, Any]
    ) -> bool:
        """Evaluate a single condition against context."""

        # Get actual value from context
        actual_value = context.get(condition_key)

        if actual_value is None:
            return False

        # Handle different condition types
        if condition_key == "mark_type":
            # Handle both single value and array of values
            if isinstance(expected_value, list):
                return str(actual_value) in expected_value
            else:
                # Case-insensitive comparison for mark types
                return str(actual_value).lower() == str(expected_value).lower()

        elif condition_key == "has_dual_axis":
            return bool(actual_value) == bool(expected_value)

        elif condition_key == "has_text_marks":
            return bool(actual_value) == bool(expected_value)

        elif condition_key == "text_encoding_has_measure":
            return bool(actual_value) == bool(expected_value)

        elif condition_key in [
            "columns_shelf_count",
            "rows_shelf_count",
            "dimensions_on_x_axis",
            "measures_on_y_axis",
        ]:
            return self._evaluate_numeric_condition(actual_value, expected_value)

        elif condition_key == "rows_shelf_has_string":
            return bool(actual_value) == bool(expected_value)

        elif condition_key in [
            "has_no_color_size_encoding",
            "has_label_encoding",
            "has_continuous_color_scale",
            "has_latitude_longitude_encoding",
            "has_hierarchical_layout",
            "has_geographic_fields",
            "has_angle_encoding",
            "has_multiple_measures",
            "has_mark_stacking",
            "has_binned_fields",
        ]:
            return bool(actual_value) == bool(expected_value)

        elif condition_key in [
            "x_encoding",
            "y_encoding",
            "color_encoding",
            "size_encoding",
        ]:
            return self._evaluate_encoding_condition(actual_value, expected_value)

        elif condition_key == "orientation":
            # For now, assume vertical orientation (this could be enhanced)
            return expected_value == "vertical"

        else:
            # Generic equality check
            return actual_value == expected_value

    def _evaluate_numeric_condition(
        self, actual: Union[int, float], expected: Union[int, float, str]
    ) -> bool:
        """Evaluate numeric conditions with comparison operators."""
        if isinstance(expected, str):
            if expected.startswith(">"):
                try:
                    threshold = int(expected[1:])
                    return actual > threshold
                except ValueError:
                    return False
            elif expected.startswith("<"):
                try:
                    threshold = int(expected[1:])
                    return actual < threshold
                except ValueError:
                    return False
            elif expected.startswith(">="):
                try:
                    threshold = int(expected[2:])
                    return actual >= threshold
                except ValueError:
                    return False
            elif expected.startswith("<="):
                try:
                    threshold = int(expected[2:])
                    return actual <= threshold
                except ValueError:
                    return False

        # Direct comparison
        try:
            return actual == int(expected)
        except (ValueError, TypeError):
            return actual == expected

    def _evaluate_encoding_condition(
        self, actual: Optional[str], expected: Union[str, List[str]]
    ) -> bool:
        """Evaluate encoding conditions."""
        if expected is None:
            return actual is None

        if isinstance(expected, list):
            return actual in expected

        return actual == expected

    def get_supported_chart_types(self) -> List[str]:
        """Get list of supported chart types from YAML config."""
        return list(self.rules.get("basic_chart_detection", {}).keys())

    def get_rule_stats(self) -> Dict[str, Any]:
        """Get statistics about loaded rules."""
        basic_rules = self.rules.get("basic_chart_detection", {})

        stats = {
            "total_rules": len(basic_rules),
            "chart_types": list(basic_rules.keys()),
            "config_file": str(self.config_path),
            "fallback_chart_type": self.rules.get("fallback", {}).get(
                "default_chart_type", "bar"
            ),
        }

        return stats
