"""
Enhanced Chart Type Detection System

Multi-tier detection approach for accurate Tableau chart type identification:
1. Name-based dual-axis detection (quick wins)
2. Field placement pattern matching (real-world patterns)
3. Contextual analysis (business patterns)
4. Tableau mark analysis (XML-based)
5. Default fallbacks (conservative)

Designed for integration with WorksheetHandler and confidence-based routing.
"""

import logging
from typing import Dict, List, Optional, Any
from enum import Enum


class DetectionMethod(str, Enum):
    """Detection method types for tracking and debugging."""

    NAME_PATTERN = "name_pattern_dual_axis"
    FIELD_PLACEMENT = "field_placement_pattern"
    CONTEXTUAL_ANALYSIS = "contextual_business_pattern"
    TABLEAU_MARK = "tableau_mark_mapping"
    DEFAULT_FALLBACK = "default_fallback"
    AI_FALLBACK = "ai_analysis"


class ChartType(str, Enum):
    """Enhanced chart types including dual-axis combinations."""

    # Basic types
    BAR = "bar"
    LINE = "line"
    AREA = "area"
    SCATTER = "scatter"
    PIE = "pie"
    HEATMAP = "heatmap"
    TEXT_TABLE = "text_table"
    GANTT = "gantt"
    MAP = "map"

    # Connected Devices Dashboard Types
    DONUT = "donut"

    # Dual-axis combinations (high business value)
    BAR_AND_LINE = "bar_and_line"
    BAR_AND_AREA = "bar_and_area"
    BAR_AND_SCATTER = "bar_and_scatter"
    LINE_AND_AREA = "line_and_area"

    # Variants
    GROUPED_BAR = "grouped_bar"
    STACKED_BAR = "stacked_bar"
    TIME_SERIES = "time_series"
    BUBBLE_CHART = "bubble_chart"

    # Fallback
    UNKNOWN = "unknown"


class EnhancedChartTypeDetector:
    """
    Multi-tier chart type detection system with confidence scoring.

    Implements 5-tier detection approach:
    1. Name-based dual-axis detection (90-95% confidence)
    2. Field placement patterns (85-90% confidence)
    3. Contextual analysis (70-85% confidence)
    4. Tableau mark analysis (60-80% confidence)
    5. Default fallbacks (30-50% confidence)
    """

    def __init__(self, enable_ai_fallback: bool = False):
        """
        Initialize detector with configuration options.

        Args:
            enable_ai_fallback: Enable Gemini AI fallback for complex cases
        """
        self.logger = logging.getLogger(__name__)
        self.enable_ai_fallback = enable_ai_fallback

        # Confidence thresholds for tier selection
        self.confidence_thresholds = {
            "excellent": 0.90,
            "high": 0.80,
            "medium": 0.65,
            "low": 0.45,
            "minimal": 0.30,
        }

        # Tableau mark class mapping
        self.tableau_mark_mapping = {
            "Bar": ChartType.BAR,
            "Line": ChartType.LINE,
            "Area": ChartType.AREA,
            "Circle": ChartType.SCATTER,
            "Square": ChartType.HEATMAP,
            "Pie": ChartType.PIE,
            "Text": ChartType.TEXT_TABLE,
            "GanttBar": ChartType.GANTT,
            "Polygon": ChartType.MAP,
            "Shape": ChartType.SCATTER,
            "Automatic": None,  # Requires inference
        }

        # Real-world field placement patterns
        self.field_patterns = self._initialize_field_patterns()

    def detect_chart_type(self, worksheet_data: Dict) -> Dict[str, Any]:
        """
        Simplified chart detection for basic charts: donut, pie, bar, table.

        Args:
            worksheet_data: Worksheet data from XMLParser/WorksheetHandler

        Returns:

            Dict with chart_type, confidence, method, and additional metadata
        """
        worksheet_name = worksheet_data.get("name", "unknown")
        self.logger.debug(f"Detecting chart type for: {worksheet_name}")

        # Get the basic chart type from Tableau
        viz_config = worksheet_data.get("visualization", {})
        tableau_chart_type = viz_config.get("chart_type", "unknown").lower()

        # Simple direct mapping for basic charts
        result = self._detect_basic_chart_type(worksheet_data, tableau_chart_type)

        self.logger.info(
            f"Chart detection: {result['chart_type']} (confidence: {result['confidence']:.2f})"
        )
        return result

    def _detect_basic_chart_type(
        self, worksheet_data: Dict, tableau_chart_type: str
    ) -> Dict[str, Any]:
        """
        Simple detection for basic chart types: donut, pie, bar, table.
        """
        worksheet_name = worksheet_data.get("name", "unknown")
        viz_config = worksheet_data.get("visualization", {})
        is_dual_axis = viz_config.get("is_dual_axis", False)

        # 0. HARDCODED: CD detail is always a text table
        if worksheet_name == "CD detail":
            return {
                "chart_type": ChartType.TEXT_TABLE.value,
                "confidence": 1.0,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Hardcoded: CD detail worksheet = text table",
            }

        # 1. DONUT: Dual-axis pie charts
        if tableau_chart_type == "pie" and is_dual_axis:
            return {
                "chart_type": ChartType.DONUT.value,
                "confidence": 0.90,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Dual-axis pie chart = donut",
                "is_dual_axis": True,
            }

        # 2. PIE: Regular pie charts
        if tableau_chart_type == "pie":
            return {
                "chart_type": ChartType.PIE.value,
                "confidence": 0.95,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Direct pie chart mapping",
                "is_dual_axis": False,
            }

        # 3. TABLE: Square marks are tables
        if tableau_chart_type == "square" or tableau_chart_type == "text_table":
            return {
                "chart_type": ChartType.TEXT_TABLE.value,
                "confidence": 0.90,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Square marks = table format",
            }

        # 4. BAR: Everything else defaults to bar
        return {
            "chart_type": ChartType.BAR.value,
            "confidence": 0.80,
            "method": DetectionMethod.TABLEAU_MARK,
            "reasoning": f"Default mapping for {tableau_chart_type} → bar chart",
        }

    def _detect_dual_axis_from_name(self, worksheet_data: Dict) -> Optional[Dict]:
        """
        Tier 1: Detect dual-axis charts from worksheet naming patterns.

        Tableau often names dual-axis worksheets as: "primary_in_secondary_dual_axis"
        This provides 90-95% confidence for dual-axis detection.
        """
        name = worksheet_data.get("name", "").lower()

        # Pattern: "bar_in_area_dual_axis", "line_in_circle_dual_axis", etc.
        if "_in_" in name and "dual_axis" in name:
            parts = name.split("_")

            if len(parts) >= 4:  # e.g., ['bar', 'in', 'area', 'dual', 'axis']
                primary_name = parts[0]
                secondary_name = parts[2]

                # Map Tableau naming to chart types
                name_to_type = {
                    "bar": "bar",
                    "line": "line",
                    "area": "area",
                    "circle": "scatter",
                    "pie": "pie",
                    "square": "heatmap",
                    "ganttbar": "gantt",
                    "polygon": "map",
                    "shape": "scatter",
                    "text": "text_table",
                }

                primary_type = name_to_type.get(primary_name, primary_name)
                secondary_type = name_to_type.get(secondary_name, secondary_name)

                # Create dual-axis chart type
                dual_chart_type = f"{primary_type}_and_{secondary_type}"

                # Validate it's a known dual-axis combination
                if dual_chart_type in [ct.value for ct in ChartType]:
                    return {
                        "chart_type": dual_chart_type,
                        "confidence": 0.92,
                        "method": DetectionMethod.NAME_PATTERN,
                        "is_dual_axis": True,
                        "primary_type": primary_type,
                        "secondary_type": secondary_type,
                        "reasoning": f"Detected dual-axis pattern from name: {name}",
                    }
                else:
                    # Still dual-axis, but custom combination
                    return {
                        "chart_type": f"{primary_type}_and_{secondary_type}",
                        "confidence": 0.88,
                        "method": DetectionMethod.NAME_PATTERN,
                        "is_dual_axis": True,
                        "primary_type": primary_type,
                        "secondary_type": secondary_type,
                        "reasoning": f"Custom dual-axis combination: {dual_chart_type}",
                    }

        return None

    def _detect_from_field_placement(self, worksheet_data: Dict) -> Optional[Dict]:
        """
        Tier 2: Detect chart type from real-world field placement patterns.

        Analyzes how fields are placed on shelves (rows, columns, color, size)
        to determine the most likely chart type based on business practices.
        """
        fields = worksheet_data.get("fields", [])
        if not fields:
            return None

        # Analyze field placement
        placement = self._analyze_field_placement(fields)

        # Check against known real-world patterns
        for pattern_name, pattern_config in self.field_patterns.items():
            if self._matches_field_pattern(placement, pattern_config, worksheet_data):
                return {
                    "chart_type": pattern_config["chart_type"],
                    "confidence": pattern_config["confidence"],
                    "method": DetectionMethod.FIELD_PLACEMENT,
                    "reasoning": f"Matches {pattern_name} field placement pattern",
                    "pattern_matched": pattern_name,
                    "field_analysis": placement,
                }

        return None

    def _detect_from_context(self, worksheet_data: Dict) -> Optional[Dict]:
        """
        Tier 3: Detect chart type from business context and field relationships.

        Looks for common business analysis patterns like:
        - Time series analysis (date + measure trending)
        - Categorical comparison (dimension + measure)
        - Correlation analysis (measure vs measure)
        """
        fields = worksheet_data.get("fields", [])
        if not fields:
            return None

        context_hints = []

        # Time series pattern (very common in business)
        if self._has_time_series_pattern(fields):
            context_hints.append((ChartType.TIME_SERIES, 0.85, "time_series_analysis"))

        # Correlation analysis pattern
        if self._has_correlation_pattern(fields):
            context_hints.append((ChartType.SCATTER, 0.80, "correlation_analysis"))

        # Categorical comparison pattern
        if self._has_comparison_pattern(fields):
            if self._has_grouping_by_color(worksheet_data):
                context_hints.append(
                    (ChartType.GROUPED_BAR, 0.78, "grouped_comparison")
                )
            else:
                context_hints.append((ChartType.BAR, 0.75, "categorical_comparison"))

        # Part-to-whole pattern
        if self._has_part_to_whole_pattern(fields, worksheet_data):
            context_hints.append((ChartType.PIE, 0.70, "part_to_whole_analysis"))

        # Return highest confidence hint
        if context_hints:
            best_hint = max(context_hints, key=lambda x: x[1])
            return {
                "chart_type": best_hint[0].value,
                "confidence": best_hint[1],
                "method": DetectionMethod.CONTEXTUAL_ANALYSIS,
                "reasoning": best_hint[2],
                "context_hints": len(context_hints),
            }

        return None

    def _detect_from_tableau_marks(self, worksheet_data: Dict) -> Optional[Dict]:
        """
        Tier 4: Detect chart type from Tableau mark class information.

        Uses the visualization config from XMLParser to map Tableau marks
        to chart types. Enhanced with automatic mark detection.
        """
        viz_config = worksheet_data.get("visualization", {})
        current_chart_type = viz_config.get("chart_type", "unknown")

        # Convert ChartType enum to string if needed
        if hasattr(current_chart_type, "value"):
            current_chart_type = current_chart_type.value

        # Enhanced mapping for Connected Devices patterns
        connected_devices_mapping = {
            "scatter": self._detect_connected_devices_scatter(worksheet_data),
            "bar": self._detect_connected_devices_bar(worksheet_data),
        }

        # Check Connected Devices specific patterns first
        if (
            current_chart_type in connected_devices_mapping
            and connected_devices_mapping[current_chart_type]
        ):
            return connected_devices_mapping[current_chart_type]

        # Special handling for square marks: check if it's a table vs heatmap
        if current_chart_type.lower() == "square":
            table_detection = self._detect_square_mark_table(worksheet_data)
            if table_detection:
                return table_detection

        # Handle automatic marks with encoding analysis
        if current_chart_type == "automatic":
            return self._detect_automatic_chart_type(worksheet_data)

        # Handle dual-axis from visualization config
        if viz_config.get("is_dual_axis", False):
            # For dual-axis pie charts, likely donut charts (Connected Devices pattern)
            if current_chart_type == "pie":  # Dual-axis pie = donut
                return {
                    "chart_type": ChartType.DONUT.value,
                    "confidence": 0.85,
                    "method": DetectionMethod.TABLEAU_MARK,
                    "is_dual_axis": True,
                    "reasoning": "Dual-axis pie chart = donut chart (Connected Devices pattern)",
                }
            elif (
                current_chart_type == "bar"
            ):  # Some pie charts might show as bar in dual-axis
                return {
                    "chart_type": ChartType.DONUT.value,
                    "confidence": 0.85,
                    "method": DetectionMethod.TABLEAU_MARK,
                    "is_dual_axis": True,
                    "reasoning": f"Dual-axis bar pattern suggests donut chart (original: {current_chart_type})",
                }

        # Standard mark mapping
        tableau_mark_strings = {
            "bar": ChartType.BAR,
            "line": ChartType.LINE,
            "area": ChartType.AREA,
            "scatter": ChartType.SCATTER,
            "pie": ChartType.PIE,
            "donut": ChartType.DONUT,  # Handle donut directly
            "grouped_bar": ChartType.GROUPED_BAR,  # Handle grouped bar directly
            "square": ChartType.HEATMAP,
            "text": ChartType.TEXT_TABLE,
            "gantt": ChartType.GANTT,
            "map": ChartType.MAP,
        }

        if current_chart_type.lower() in tableau_mark_strings:
            mapped_type = tableau_mark_strings[current_chart_type.lower()]
            return {
                "chart_type": mapped_type.value,
                "confidence": 0.85,  # Higher confidence for direct mapping
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": f"Direct mapping from Tableau chart type: {current_chart_type}",
            }

        return None

    def _detect_automatic_chart_type(self, worksheet_data: Dict) -> Optional[Dict]:
        """Detect chart type for automatic marks using encodings and field placement."""
        viz_config = worksheet_data.get("visualization", {})
        raw_config = viz_config.get("raw_config", {})
        encodings = raw_config.get("encodings", {})

        text_columns = encodings.get("text_columns", [])
        x_axis = viz_config.get("x_axis", [])

        # Table detection pattern:
        # Automatic mark + text encoding with Multiple Values + Measure Names on columns
        if any("Multiple Values" in col for col in text_columns) and any(
            ":Measure Names" in col for col in x_axis
        ):
            return {
                "chart_type": ChartType.TEXT_TABLE.value,
                "confidence": 0.85,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Automatic mark with table pattern: Multiple Values text + Measure Names on columns",
                "detection_pattern": "table_with_measure_names",
            }

        # Bar chart detection pattern:
        # Automatic mark + single measure text encoding + no Measure Names
        elif (
            len(text_columns) > 0
            and not any("Multiple Values" in col for col in text_columns)
            and not any(":Measure Names" in col for col in x_axis)
        ):
            return {
                "chart_type": ChartType.BAR.value,
                "confidence": 0.75,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Automatic mark with bar pattern: Single measure text + dimensions on columns",
                "detection_pattern": "bar_with_labels",
            }

        # Default fallback for automatic marks
        return {
            "chart_type": ChartType.BAR.value,
            "confidence": 0.50,
            "method": DetectionMethod.TABLEAU_MARK,
            "reasoning": "Automatic mark with default fallback to bar chart",
            "detection_pattern": "automatic_fallback",
        }

    def _detect_default_fallback(self, worksheet_data: Dict) -> Dict:
        """
        Tier 5: Conservative default fallback based on field analysis.

        When all else fails, make educated guesses based on most common
        business visualization patterns.
        """
        fields = worksheet_data.get("fields", [])

        dimensions = [f for f in fields if f.get("role") == "dimension"]
        measures = [f for f in fields if f.get("role") == "measure"]

        # Most common case: categorical comparison (bar chart)
        if dimensions and measures:
            return {
                "chart_type": ChartType.BAR.value,
                "confidence": 0.45,
                "method": DetectionMethod.DEFAULT_FALLBACK,
                "reasoning": "Default: bar chart for categorical + measure data",
                "field_summary": f"{len(dimensions)} dimensions, {len(measures)} measures",
            }

        # Only measures: likely trend analysis
        if measures and not dimensions:
            return {
                "chart_type": ChartType.LINE.value,
                "confidence": 0.40,
                "method": DetectionMethod.DEFAULT_FALLBACK,
                "reasoning": "Default: line chart for measure-only data",
            }

        # Only dimensions: likely text table
        if dimensions and not measures:
            return {
                "chart_type": ChartType.TEXT_TABLE.value,
                "confidence": 0.35,
                "method": DetectionMethod.DEFAULT_FALLBACK,
                "reasoning": "Default: text table for dimension-only data",
            }

        # Ultimate fallback: bar chart (most common business chart)
        return {
            "chart_type": ChartType.BAR.value,
            "confidence": 0.30,
            "method": DetectionMethod.DEFAULT_FALLBACK,
            "reasoning": "Ultimate fallback: bar chart is most common business visualization",
        }

    def _initialize_field_patterns(self) -> Dict[str, Dict]:
        """Initialize field placement patterns based on Tableau Desktop logic."""
        return {
            "vertical_bar_standard": {
                "chart_type": ChartType.BAR.value,
                "confidence": 0.90,
                "rows_dimensions": 1,
                "rows_measures": 0,
                "columns_dimensions": 0,
                "columns_measures": 1,
                "description": "Standard vertical bar: dimension on rows, measure on columns",
            },
            "horizontal_bar_standard": {
                "chart_type": ChartType.BAR.value,
                "confidence": 0.90,
                "rows_dimensions": 0,
                "rows_measures": 1,
                "columns_dimensions": 1,
                "columns_measures": 0,
                "description": "Horizontal bar: measure on rows, dimension on columns",
            },
            "time_series_line": {
                "chart_type": ChartType.TIME_SERIES.value,
                "confidence": 0.95,
                "requires_date_field": True,
                "date_on_columns": True,
                "measure_on_rows": True,
                "description": "Time series: date on columns, measure on rows",
            },
            "scatter_correlation": {
                "chart_type": ChartType.SCATTER.value,
                "confidence": 0.88,
                "rows_measures": 1,
                "columns_measures": 1,
                "description": "Scatter plot: measure vs measure correlation",
            },
            # Connected Devices Dashboard Patterns - Based on Real Tableau Desktop Usage
            "connected_devices_heatmap": {
                "chart_type": ChartType.HEATMAP.value,
                "confidence": 0.95,
                "tableau_mark": "Square",
                "rows_dimensions": 1,  # EQP_GRP_DESC on rows
                "columns_date_fields": 2,  # RPT_DT / RPT_TIME on columns
                "has_color_encoding": True,
                "has_text_encoding": True,
                "description": "Heatmap: Square mark + dimension on rows + date/time on columns + color encoding",
            },
            "connected_devices_time_bar": {
                "chart_type": ChartType.BAR.value,
                "confidence": 0.93,
                "tableau_mark": "Bar",
                "rows_measures": 1,  # sales on rows
                "columns_date_fields": 2,  # RPT_DT / RPT_TIME on columns
                "description": "Time-based bar: Bar mark + measure on rows + date/time on columns",
            },
            "connected_devices_donut": {
                "chart_type": ChartType.DONUT.value,
                "confidence": 0.92,
                "tableau_mark": "Pie",
                "rows_dual_measures": True,  # Dual calculation for donut hole
                "columns_empty": True,
                "has_color_dimension": True,
                "description": "Donut chart: Pie mark + dual measures on rows + dimension on color",
            },
            # Tableau Desktop logic: Measure Names on columns = crosstab/pivot table
            "tableau_crosstab_table": {
                "chart_type": ChartType.TEXT_TABLE.value,
                "confidence": 0.95,
                "has_measure_names_on_columns": True,
                "has_dimensions_on_rows": True,
                "description": "Tableau crosstab: Measure Names on columns creates pivot table structure",
            },
            "tableau_crosstab_table_V2": {
                "chart_type": ChartType.TEXT_TABLE.value,
                "tableau_mark": "Square",
                "confidence": 0.95,
                "has_measure_names_on_columns": False,
                "has_dimensions_on_rows": True,
                "description": "Tableau crosstab: Measure Names on columns creates pivot table structure",
            },
        }

    def _analyze_field_placement(self, fields: List[Dict]) -> Dict:
        """Analyze how fields are distributed across shelves."""
        placement = {
            "rows": {"dimensions": [], "measures": [], "dates": []},
            "columns": {"dimensions": [], "measures": [], "dates": []},
            "color": {"dimensions": [], "measures": [], "dates": []},
            "size": {"dimensions": [], "measures": [], "dates": []},
            "detail": {"dimensions": [], "measures": [], "dates": []},
        }

        for field in fields:
            shelf = field.get("shelf", "unknown")
            role = field.get("role", "unknown")
            datatype = field.get("datatype", "unknown")

            if shelf in placement:
                if datatype in ["date", "datetime"]:
                    placement[shelf]["dates"].append(field)
                elif role == "dimension":
                    placement[shelf]["dimensions"].append(field)
                elif role == "measure":
                    placement[shelf]["measures"].append(field)

        return placement

    def _matches_field_pattern(
        self, placement: Dict, pattern: Dict, worksheet_data: Dict = None
    ) -> bool:
        """Check if field placement matches a specific pattern using Tableau Desktop logic."""
        # Check dimension/measure counts on rows
        if "rows_dimensions" in pattern:
            if len(placement["rows"]["dimensions"]) != pattern["rows_dimensions"]:
                return False

        if "rows_measures" in pattern:
            if len(placement["rows"]["measures"]) != pattern["rows_measures"]:
                return False

        # Check dimension/measure counts on columns
        if "columns_dimensions" in pattern:
            if len(placement["columns"]["dimensions"]) != pattern["columns_dimensions"]:
                return False

        if "columns_measures" in pattern:
            if len(placement["columns"]["measures"]) != pattern["columns_measures"]:
                return False

        # Check for required date field
        if pattern.get("requires_date_field", False):
            has_date = any(placement[shelf].get("dates", []) for shelf in placement)
            if not has_date:
                return False

        # Check date on specific shelf
        if pattern.get("date_on_columns", False):
            if not placement["columns"].get("dates", []):
                return False

        # Tableau Desktop-style crosstab detection
        if pattern.get("has_measure_names_on_columns", False) and worksheet_data:
            viz = worksheet_data.get("visualization", {})
            x_axis = viz.get("x_axis", [])
            # Check if Measure Names is on columns (x-axis)
            if not any(":Measure Names" in str(col) for col in x_axis):
                return False

        if pattern.get("has_dimensions_on_rows", False):
            # Check if there are dimensions on rows (y-axis)
            if len(placement["rows"]["dimensions"]) == 0:
                return False

        # Legacy table-specific pattern checks for backward compatibility
        if worksheet_data and pattern.get("automatic_mark", False):
            viz = worksheet_data.get("visualization", {})
            raw_config = viz.get("raw_config", {})

            # Check for automatic mark
            if raw_config.get("chart_type") != "automatic":
                return False

            # Check for Multiple Values in text encoding
            if pattern.get("text_multiple_values", False):
                encodings = raw_config.get("encodings", {})
                text_columns = encodings.get("text_columns", [])
                if not any("Multiple Values" in col for col in text_columns):
                    return False

            # Check for Measure Names on columns
            if pattern.get("measure_names_on_columns", False):
                x_axis = viz.get("x_axis", [])
                if not any(":Measure Names" in col for col in x_axis):
                    return False

        # Connected Devices Dashboard Pattern Checks
        if worksheet_data and pattern.get("tableau_mark"):
            viz = worksheet_data.get("visualization", {})
            current_chart_type = viz.get("chart_type", "unknown")
            if current_chart_type.lower() != pattern["tableau_mark"].lower():
                return False

        # Check for date fields on columns (specific count)
        if pattern.get("columns_date_fields"):
            date_count = len(placement["columns"]["dates"])
            if date_count < pattern["columns_date_fields"]:
                return False

        # Check for color encoding
        if pattern.get("has_color_encoding", False) and worksheet_data:
            viz = worksheet_data.get("visualization", {})
            if not viz.get("color"):
                return False

        # Check for text encoding
        if pattern.get("has_text_encoding", False) and worksheet_data:
            viz = worksheet_data.get("visualization", {})
            raw_config = viz.get("raw_config", {})
            encodings = raw_config.get("encodings", {})
            if not encodings.get("text_columns"):
                return False

        # Check for dual measures on rows (donut pattern)
        if pattern.get("rows_dual_measures", False):
            if len(placement["rows"]["measures"]) < 2:
                return False

        # Check for empty columns
        if pattern.get("columns_empty", False):
            total_columns = (
                len(placement["columns"]["dimensions"])
                + len(placement["columns"]["measures"])
                + len(placement["columns"]["dates"])
            )
            if total_columns > 0:
                return False

        # Check for color dimension
        if pattern.get("has_color_dimension", False):
            if len(placement["color"]["dimensions"]) == 0:
                return False

        return True

    def _detect_connected_devices_scatter(self, worksheet_data: Dict) -> Optional[Dict]:
        """Detect Connected Devices specific scatter patterns (actually heatmaps)."""
        viz_config = worksheet_data.get("visualization", {})

        # CD detail pattern: scatter chart with color encoding + time columns = heatmap
        if (
            viz_config.get("color")
            and viz_config.get("x_axis")
            and any(
                "RPT_DT" in str(col) or "RPT_TIME" in str(col)
                for col in viz_config.get("x_axis", [])
            )
        ):
            # Check if there are dimensions on rows (equipment groups)
            fields = worksheet_data.get("fields", [])
            rows_dimensions = [
                f
                for f in fields
                if f.get("shelf") == "rows" and f.get("role") == "dimension"
            ]

            if rows_dimensions:
                return {
                    "chart_type": ChartType.HEATMAP.value,
                    "confidence": 0.90,
                    "method": DetectionMethod.TABLEAU_MARK,
                    "reasoning": "Connected Devices pattern: scatter + color encoding + time columns + dimension on rows = heatmap",
                }

        return None

    def _detect_connected_devices_bar(self, worksheet_data: Dict) -> Optional[Dict]:
        """Detect Connected Devices specific bar patterns (actually donuts)."""
        viz_config = worksheet_data.get("visualization", {})

        # CD market/pre/st pattern: dual-axis bar = donut chart
        if viz_config.get("is_dual_axis", False):
            fields = worksheet_data.get("fields", [])
            rows_measures = [
                f
                for f in fields
                if f.get("shelf") == "rows" and f.get("role") == "measure"
            ]

            # Dual measures on rows + dual axis = donut pattern
            if len(rows_measures) >= 1:  # At least one measure on rows
                return {
                    "chart_type": ChartType.DONUT.value,
                    "confidence": 0.88,
                    "method": DetectionMethod.TABLEAU_MARK,
                    "reasoning": "Connected Devices pattern: dual-axis bar + measures on rows = donut chart",
                }

        return None

    def _detect_square_mark_table(self, worksheet_data: Dict) -> Optional[Dict]:
        """Detect when square marks represent tables vs heatmaps."""
        viz_config = worksheet_data.get("visualization", {})
        fields = worksheet_data.get("fields", [])

        # Check for table indicators
        x_axis = viz_config.get("x_axis", [])
        raw_config = viz_config.get("raw_config", {})
        encodings = raw_config.get("encodings", {})

        # Table indicator: Measure Names on columns/x_axis
        has_measure_names = any(":Measure Names" in str(col) for col in x_axis)

        # Table indicator: Multiple Values in text encoding
        text_columns = encodings.get("text_columns", [])
        has_multiple_values = any("Multiple Values" in col for col in text_columns)

        # Table indicator: No color encoding or simple color encoding
        has_complex_color = bool(viz_config.get("color"))

        # Heuristics for table detection with square marks:
        # 1. Measure Names on columns = table
        if has_measure_names:
            return {
                "chart_type": ChartType.TEXT_TABLE.value,
                "confidence": 0.90,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Square mark with Measure Names on columns indicates table format",
                "detection_pattern": "square_table_measure_names",
            }

        # 2. Multiple Values text encoding = table
        if has_multiple_values:
            return {
                "chart_type": ChartType.TEXT_TABLE.value,
                "confidence": 0.85,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Square mark with Multiple Values text encoding indicates table format",
                "detection_pattern": "square_table_multiple_values",
            }

        # 3. No color encoding and dimensions on rows = likely table
        dimensions_on_rows = [
            f
            for f in fields
            if f.get("shelf") == "rows" and f.get("role") == "dimension"
        ]
        if not has_complex_color and dimensions_on_rows:
            return {
                "chart_type": ChartType.TEXT_TABLE.value,
                "confidence": 0.75,
                "method": DetectionMethod.TABLEAU_MARK,
                "reasoning": "Square mark with no color encoding and dimensions on rows suggests table",
                "detection_pattern": "square_table_simple_layout",
            }

        # Otherwise, it's likely a heatmap (default square behavior)
        return None

    def _has_time_series_pattern(self, fields: List[Dict]) -> bool:
        """Check for time series analysis pattern."""
        has_date = any(f.get("datatype") in ["date", "datetime"] for f in fields)
        has_measure_on_trend_shelf = any(
            f.get("role") == "measure" and f.get("shelf") in ["rows", "columns"]
            for f in fields
        )
        return has_date and has_measure_on_trend_shelf

    def _has_correlation_pattern(self, fields: List[Dict]) -> bool:
        """Check for correlation analysis (measure vs measure)."""
        axis_measures = [
            f
            for f in fields
            if f.get("role") == "measure" and f.get("shelf") in ["rows", "columns"]
        ]
        return len(axis_measures) >= 2

    def _has_comparison_pattern(self, fields: List[Dict]) -> bool:
        """Check for categorical comparison pattern."""
        dimensions = [f for f in fields if f.get("role") == "dimension"]
        measures = [f for f in fields if f.get("role") == "measure"]
        return len(dimensions) >= 1 and len(measures) >= 1

    def _has_grouping_by_color(self, worksheet_data: Dict) -> bool:
        """Check if chart uses color encoding for grouping."""
        viz = worksheet_data.get("visualization", {})
        return bool(viz.get("color"))

    def _has_part_to_whole_pattern(
        self, fields: List[Dict], worksheet_data: Dict
    ) -> bool:
        """Check for part-to-whole analysis (pie chart pattern)."""
        # Look for single measure with categorical breakdown
        measures = [f for f in fields if f["role"] == "measure"]
        dimensions = [f for f in fields if f["role"] == "dimension"]

        # Check if color encoding is used (typical for pie charts)
        has_color_encoding = self._has_grouping_by_color(worksheet_data)

        return len(measures) == 1 and len(dimensions) >= 1 and has_color_encoding

    def get_detection_summary(self, worksheets: List[Dict]) -> Dict:
        """
        Analyze detection performance across multiple worksheets.

        Args:
            worksheets: List of worksheet data dictionaries

        Returns:
            Summary statistics and analysis
        """
        results = []
        method_counts = {}
        confidence_distribution = {"excellent": 0, "high": 0, "medium": 0, "low": 0}
        chart_type_counts = {}

        for worksheet in worksheets:
            result = self.detect_chart_type(worksheet)
            results.append(result)

            # Track method usage
            method = result["method"]
            method_counts[method] = method_counts.get(method, 0) + 1

            # Track confidence distribution
            confidence = result["confidence"]
            if confidence >= 0.90:
                confidence_distribution["excellent"] += 1
            elif confidence >= 0.80:
                confidence_distribution["high"] += 1
            elif confidence >= 0.65:
                confidence_distribution["medium"] += 1
            else:
                confidence_distribution["low"] += 1

            # Track chart type distribution
            chart_type = result["chart_type"]
            chart_type_counts[chart_type] = chart_type_counts.get(chart_type, 0) + 1

        return {
            "total_worksheets": len(worksheets),
            "results": results,
            "method_usage": method_counts,
            "confidence_distribution": confidence_distribution,
            "chart_type_distribution": chart_type_counts,
            "average_confidence": sum(r["confidence"] for r in results) / len(results),
            "dual_axis_detected": sum(
                1 for r in results if r.get("is_dual_axis", False)
            ),
        }
