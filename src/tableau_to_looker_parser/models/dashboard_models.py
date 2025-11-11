"""
Dashboard data models for Tableau to LookML migration.

Contains Pydantic models for dashboard definitions, element positioning, and layout configurations.
Self-contained models that embed all necessary information.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum

from .position_models import Position, Style
from .worksheet_models import WorksheetSchema


class ElementType(str, Enum):
    """Types of elements that can appear in a dashboard."""

    WORKSHEET = "worksheet"
    FILTER = "filter"
    PARAMETER = "parameter"
    LEGEND = "legend"
    TITLE = "title"
    TEXT = "text"
    IMAGE = "image"
    WEB = "web"
    BLANK = "blank"
    CUSTOM = "custom"  # For future extensibility


class DashboardElement(BaseModel):
    """
    Universal dashboard element that can contain any type of content.
    Self-contained with all positioning, styling, and content information.
    """

    # Identity
    element_id: str = Field(
        ..., description="Unique element identifier within dashboard"
    )
    element_type: ElementType = Field(
        ..., description="Type of content this element contains"
    )

    # Universal positioning and styling (always present)
    position: Position = Field(
        ..., description="Element position and size within dashboard"
    )
    style: Style = Field(default_factory=Style, description="Element visual styling")

    # Content (one of these will be populated based on element_type)
    worksheet: Optional[WorksheetSchema] = Field(
        None, description="Worksheet content (if element_type=worksheet)"
    )
    filter_config: Optional[Dict[str, Any]] = Field(
        None, description="Filter configuration (if element_type=filter)"
    )
    parameter_config: Optional[Dict[str, Any]] = Field(
        None, description="Parameter config (if element_type=parameter)"
    )
    text_content: Optional[str] = Field(
        None, description="Text content (if element_type=text)"
    )
    image_config: Optional[Dict[str, Any]] = Field(
        None, description="Image configuration (if element_type=image)"
    )

    # Interaction configuration
    is_interactive: bool = Field(
        default=True, description="Whether element responds to user interactions"
    )
    interactions: List[Dict[str, Any]] = Field(
        default_factory=list, description="Interaction configurations"
    )

    # Extensibility for future element types
    custom_content: Dict[str, Any] = Field(
        default_factory=dict, description="Custom element content for unknown types"
    )

    def get_content(self) -> Any:
        """Get the actual content based on element type."""
        content_map = {
            ElementType.WORKSHEET: self.worksheet,
            ElementType.FILTER: self.filter_config,
            ElementType.PARAMETER: self.parameter_config,
            ElementType.TEXT: self.text_content,
            ElementType.IMAGE: self.image_config,
            ElementType.CUSTOM: self.custom_content,
        }
        return content_map.get(self.element_type)


class DashboardFilter(BaseModel):
    """Dashboard-level filter that affects multiple elements."""

    name: str = Field(..., description="Filter name/identifier")
    title: str = Field(..., description="Display title for the filter")
    field: str = Field(..., description="Field name being filtered")
    filter_type: str = Field(
        ..., description="Filter type: field_filter, date_filter, etc."
    )
    default_value: Optional[str] = Field(None, description="Default filter value")
    explore: str = Field(..., description="Source explore for filter options")

    # Multi-target filtering
    applies_to: List[str] = Field(
        default_factory=list, description="Element IDs this filter affects"
    )
    is_global: bool = Field(
        default=True, description="Whether filter affects all compatible elements"
    )


class DashboardSchema(BaseModel):
    """
    Complete dashboard definition with all elements and their positioning.
    Self-contained - no external lookups needed for any information.
    """

    # Identity
    name: str = Field(..., description="Dashboard name as it appears in Tableau")
    clean_name: str = Field(..., description="LookML-safe name (snake_case, no spaces)")
    title: str = Field(..., description="Display title for the dashboard")

    # Layout configuration
    canvas_size: Dict[str, int] = Field(
        ..., description="Dashboard canvas dimensions in pixels"
    )
    layout_type: str = Field(
        default="free_form", description="Layout strategy: free_form, newspaper, grid"
    )

    # All dashboard elements (self-contained, no lookups needed)
    elements: List[DashboardElement] = Field(
        default_factory=list, description="All dashboard elements with positioning"
    )

    # Global dashboard features
    global_filters: List[DashboardFilter] = Field(
        default_factory=list, description="Dashboard-level filters"
    )
    cross_filter_enabled: bool = Field(
        default=True, description="Whether cross-filtering between elements is enabled"
    )

    # Responsive design configuration
    responsive_config: Dict[str, Any] = Field(
        default_factory=dict, description="Mobile/tablet layout configurations"
    )

    toggles: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Toggle information for zones with name attributes",
    )
    dynamic_toggle: bool = Field(
        default=False,
        description="Whether dashboard has dynamic toggle (true if any toggle has toggle: true)",
    )

    # Processing metadata
    confidence: float = Field(
        default=0.85, description="Handler confidence score (0.0-1.0)"
    )
    parsing_errors: List[str] = Field(
        default_factory=list, description="Any parsing errors encountered"
    )

    # Extensibility for future features
    custom_properties: Dict[str, Any] = Field(
        default_factory=dict, description="Custom properties for extensibility"
    )

    # Convenience methods for accessing elements (no lookups - just filtering)
    def get_worksheet_elements(self) -> List[DashboardElement]:
        """Get all elements that contain worksheets."""
        return [e for e in self.elements if e.element_type == ElementType.WORKSHEET]

    def get_filter_elements(self) -> List[DashboardElement]:
        """Get all elements that contain filters."""
        return [e for e in self.elements if e.element_type == ElementType.FILTER]

    def get_parameter_elements(self) -> List[DashboardElement]:
        """Get all elements that contain parameters."""
        return [e for e in self.elements if e.element_type == ElementType.PARAMETER]

    def get_element_by_id(self, element_id: str) -> Optional[DashboardElement]:
        """Get specific element by ID (O(n) but typically small n)."""
        return next((e for e in self.elements if e.element_id == element_id), None)

    def get_worksheet_names(self) -> List[str]:
        """Get names of all worksheets referenced in this dashboard."""
        worksheet_names = []
        for element in self.get_worksheet_elements():
            if element.worksheet and element.worksheet.name:
                worksheet_names.append(element.worksheet.name)
        return list(set(worksheet_names))  # Remove duplicates
