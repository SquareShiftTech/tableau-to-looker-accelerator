"""
Worksheet data models for Tableau to LookML migration.

Contains Pydantic models for worksheet definitions, field usage, and visualization configurations.
Self-contained models that don't require external lookups.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum

# Import shared models (defined in separate files)
from .position_models import Position, Style


class ChartType(str, Enum):
    """Tableau chart/mark types."""

    BAR = "bar"
    COLUMN = "column"  # Column charts (vertical bars)
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    MAP = "map"
    TEXT = "text"
    TEXT_TABLE = "text_table"  # Crosstab/pivot table
    AREA = "area"
    GANTT = "gantt"
    HEATMAP = "heatmap"
    TIME_SERIES = "time_series"  # Time series charts
    # Connected Devices Dashboard Types
    DONUT = "donut"
    # Grouped/stacked variations
    GROUPED_BAR = "grouped_bar"
    STACKED_BAR = "stacked_bar"
    # Dual-axis combinations
    BAR_AND_LINE = "bar_and_line"
    BAR_AND_AREA = "bar_and_area"
    BAR_AND_SCATTER = "bar_and_scatter"
    LINE_AND_SCATTER = "line_and_scatter"
    # Other variations
    CIRCLE = "circle"
    UNKNOWN = "unknown"


class FieldReference(BaseModel):
    """Complete field reference with all metadata needed for LookML generation."""

    name: str = Field(..., description="Clean field name for LookML (e.g., 'category')")
    original_name: str = Field(
        ...,
        description="Original Tableau field name with brackets (e.g., '[Category]')",
    )
    tableau_instance: str = Field(
        ..., description="Tableau internal instance name (e.g., '[none:Category:nk]')"
    )

    # Data properties
    datatype: str = Field(
        ...,
        description="Field datatype: string, integer, real, boolean, date, datetime",
    )
    role: str = Field(..., description="Field role: dimension or measure")
    aggregation: Optional[str] = Field(
        None, description="Aggregation type for measures: sum, avg, count, min, max"
    )

    # Usage context
    shelf: str = Field(
        ..., description="Shelf placement: rows, columns, color, size, detail, etc."
    )
    encodings: List[str] = Field(
        default_factory=list,
        description="List of encoding types: text, color, size, tooltip, etc.",
    )
    derivation: str = Field(
        default="None", description="Tableau derivation like 'Sum', 'None', 'Avg'"
    )

    # LookML generation hints
    suggested_type: Optional[str] = Field(
        None, description="Suggested LookML field type"
    )
    drill_fields: List[str] = Field(
        default_factory=list, description="Suggested drill-down fields"
    )
    display_label: str = Field(
        ...,
        description="Human-readable label for LookML (from Tableau caption or cleaned original name)",
    )


class VisualizationConfig(BaseModel):
    """Complete visualization configuration for a worksheet."""

    chart_type: ChartType = Field(..., description="Primary chart type")

    # Field mappings to visual properties
    x_axis: List[str] = Field(
        default_factory=list, description="Fields on X-axis (columns shelf)"
    )
    y_axis: List[str] = Field(
        default_factory=list, description="Fields on Y-axis (rows shelf)"
    )
    color: Optional[str] = Field(None, description="Field used for color encoding")
    size: Optional[str] = Field(None, description="Field used for size encoding")
    detail: List[str] = Field(
        default_factory=list, description="Fields used for detail/grouping"
    )
    tooltip: List[str] = Field(
        default_factory=list, description="Fields shown in tooltip"
    )

    # Chart-specific properties
    is_dual_axis: bool = Field(
        default=False, description="Whether chart uses dual axis"
    )
    secondary_chart_type: Optional[ChartType] = Field(
        None, description="Secondary chart type for dual axis"
    )
    stacked: bool = Field(default=False, description="Whether visualization is stacked")

    # Visual properties
    show_labels: bool = Field(default=False, description="Whether to show data labels")
    show_totals: bool = Field(
        default=False, description="Whether to show row/column totals"
    )
    sort_fields: List[Dict[str, str]] = Field(
        default_factory=list, description="Sort configuration"
    )

    # Enhanced detection metadata
    enhanced_detection: Optional[Dict[str, Any]] = Field(
        None, description="Enhanced chart type detection metadata"
    )

    # YAML rule-based detection metadata
    yaml_detection: Optional[Dict[str, Any]] = Field(
        None, description="YAML rule-based chart type detection metadata"
    )

    # Raw Tableau configuration for unknown properties
    raw_config: Dict[str, Any] = Field(
        default_factory=dict, description="Raw Tableau configuration"
    )


class DashboardPlacement(BaseModel):
    """Information about where and how a worksheet appears in a dashboard."""

    dashboard_name: str = Field(
        ..., description="Name of the dashboard containing this worksheet"
    )
    zone_id: str = Field(..., description="Unique zone identifier within the dashboard")

    # Position and styling (embedded, no lookups needed)
    position: "Position" = Field(
        ..., description="Exact position and size in dashboard"
    )
    style: "Style" = Field(
        default_factory=lambda: Style(), description="Visual styling for this placement"
    )

    # Interaction configuration
    is_interactive: bool = Field(
        default=True, description="Whether worksheet responds to clicks/filters"
    )
    filter_targets: List[str] = Field(
        default_factory=list, description="Zone IDs that this worksheet filters"
    )
    filter_sources: List[str] = Field(
        default_factory=list, description="Zone IDs that filter this worksheet"
    )

    # Dashboard-specific overrides
    title_override: Optional[str] = Field(
        None, description="Custom title for this dashboard placement"
    )
    filters_override: List[Dict[str, Any]] = Field(
        default_factory=list, description="Dashboard-specific filters"
    )


class WorksheetSchema(BaseModel):
    """
    Complete worksheet definition with all information needed for LookML generation.
    Self-contained - no external lookups required.
    """

    # Identity
    name: str = Field(..., description="Worksheet name as it appears in Tableau")
    clean_name: str = Field(..., description="LookML-safe name (snake_case, no spaces)")
    title: str = Field(
        default="", description="Human-readable worksheet title from Tableau"
    )
    datasource_id: str = Field(..., description="ID of the connected datasource")

    # Complete field usage (no external references needed)
    fields: List[FieldReference] = Field(
        default_factory=list, description="All fields used in this worksheet"
    )
    calculated_fields: List[str] = Field(
        default_factory=list, description="Names of calculated fields used"
    )

    # Complete visualization configuration
    visualization: VisualizationConfig = Field(
        ..., description="Complete visualization settings"
    )

    # Filters and interactions
    filters: List[Dict[str, Any]] = Field(
        default_factory=list, description="Worksheet-level filters"
    )
    actions: List[Dict[str, Any]] = Field(
        default_factory=list, description="Worksheet actions and interactions"
    )

    # Dashboard usage (self-contained - no lookups needed!)
    dashboard_placements: List[DashboardPlacement] = Field(
        default_factory=list,
        description="All dashboards where this worksheet appears, with positioning",
    )

    # LookML generation metadata
    suggested_explore_joins: List[str] = Field(
        default_factory=list, description="Suggested join relationships"
    )
    performance_hints: Dict[str, Any] = Field(
        default_factory=dict, description="Performance optimization hints"
    )
    identified_measures: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Worksheet-specific measures identified from field aggregations",
    )
    derived_fields: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Derived fields identified from Tableau instances (time functions, aggregations)",
    )

    # Processing metadata
    confidence: float = Field(
        default=0.9, description="Handler confidence score (0.0-1.0)"
    )
    parsing_errors: List[str] = Field(
        default_factory=list, description="Any parsing errors encountered"
    )

    # Extensibility for future features
    custom_properties: Dict[str, Any] = Field(
        default_factory=dict, description="Custom properties for extensibility"
    )


# Update forward references
DashboardPlacement.model_rebuild()
