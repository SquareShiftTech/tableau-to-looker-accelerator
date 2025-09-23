"""
DashboardHandler for converting raw XML parser output to DashboardSchema format.

Transforms the raw dashboard data from xml_parser_v2.extract_dashboards()
into validated DashboardSchema objects.
"""

from typing import Dict, List
from ..handlers.base_handler import BaseHandler
from ..models.dashboard_models import DashboardSchema, ElementType


class DashboardHandler(BaseHandler):
    """
    Handler for Tableau dashboard elements.

    Converts raw XML parser output into DashboardSchema-compliant JSON.
    Handles element positioning, styling, and cross-references.
    """

    def can_handle(self, data: Dict) -> float:
        """Check if data contains dashboard information."""
        if not isinstance(data, dict):
            return 0.0

        # Must have basic dashboard structure
        required_keys = ["name", "canvas_size", "elements"]
        if not all(key in data for key in required_keys):
            return 0.0

        # Check canvas size structure
        canvas_size = data.get("canvas_size", {})
        if (
            not isinstance(canvas_size, dict)
            or "width" not in canvas_size
            or "height" not in canvas_size
        ):
            return 0.0

        # Check elements structure
        elements = data.get("elements", [])
        if not isinstance(elements, list):
            return 0.0

        # High confidence if it has typical dashboard elements
        confidence = 0.8

        # Boost confidence for good element data
        if elements and all(
            isinstance(elem, dict) and "element_id" in elem for elem in elements
        ):
            confidence += 0.1

        # Boost confidence for position data
        if elements and all("position" in elem for elem in elements):
            confidence += 0.1

        return min(confidence, 1.0)

    def convert_to_json(self, data: Dict) -> Dict:
        """Convert raw dashboard data to DashboardSchema-compliant JSON."""

        # Extract basic properties
        name = data["name"]
        clean_name = data.get("clean_name", self._clean_name(name))
        title = data.get("title", name.replace("_", " ").title())

        # Process canvas size
        canvas_size = self._process_canvas_size(data.get("canvas_size", {}))

        # Process elements (this is the complex part)
        elements = self._process_elements(data.get("elements", []))

        # Process global filters
        global_filters = self._process_global_filters(data.get("global_filters", []))

        # Extract layout type
        layout_type = data.get("layout_type", "newspaper")  # Tableau's default

        # Process responsive configuration
        responsive_config = data.get("responsive_config", {})

        # Calculate confidence
        confidence = self._calculate_dashboard_confidence(data, elements)

        # Build DashboardSchema data
        dashboard_data = {
            "name": name,
            "clean_name": clean_name,
            "title": title,
            "canvas_size": canvas_size,
            "layout_type": layout_type,
            "elements": elements,
            "global_filters": global_filters,
            "cross_filter_enabled": True,  # Default assumption
            "responsive_config": responsive_config,
            "confidence": confidence,
            "parsing_errors": [],
            "custom_properties": {},
        }

        # Validate with Pydantic schema
        try:
            dashboard = DashboardSchema(**dashboard_data)
            return dashboard.model_dump()
        except Exception as e:
            # If validation fails, return with lower confidence and error
            dashboard_data["confidence"] = 0.3
            dashboard_data["parsing_errors"] = [f"Schema validation failed: {str(e)}"]
            return dashboard_data

    def _process_canvas_size(self, raw_canvas: Dict) -> Dict[str, int]:
        """Process raw canvas size data."""
        return {
            "width": int(raw_canvas.get("width", 1000)),
            "height": int(raw_canvas.get("height", 800)),
            "min_width": int(
                raw_canvas.get("min_width", raw_canvas.get("width", 1000))
            ),
            "min_height": int(
                raw_canvas.get("min_height", raw_canvas.get("height", 800))
            ),
        }

    def _process_elements(self, raw_elements: List[Dict]) -> List[Dict]:
        """Process raw element data into DashboardElement format."""
        processed_elements = []

        for raw_element in raw_elements:
            if not isinstance(raw_element, dict) or "element_id" not in raw_element:
                continue

            # Determine element type
            element_type = self._determine_element_type(raw_element)

            # Process position
            position = self._process_position(raw_element.get("position", {}))

            # Process style
            style = self._process_style(raw_element.get("style", {}))

            # Process content based on type
            content_data = self._process_element_content(raw_element, element_type)

            # Build element
            element_data = {
                "element_id": raw_element["element_id"],
                "element_type": element_type.value,
                "position": position,
                "style": style,
                "is_interactive": raw_element.get("is_interactive", True),
                "interactions": raw_element.get("interactions", []),
                "custom_content": {},
            }

            # Add type-specific content
            element_data.update(content_data)

            processed_elements.append(element_data)

        return processed_elements

    def _determine_element_type(self, raw_element: Dict) -> ElementType:
        """Determine the element type from raw data."""
        element_type_str = raw_element.get("element_type", "").lower()

        # Direct mapping
        type_mapping = {
            "worksheet": ElementType.WORKSHEET,
            "filter": ElementType.FILTER,
            "parameter": ElementType.PARAMETER,
            "legend": ElementType.LEGEND,
            "title": ElementType.TITLE,
            "text": ElementType.TEXT,
            "image": ElementType.IMAGE,
            "web": ElementType.WEB,
            "blank": ElementType.BLANK,
        }

        if element_type_str in type_mapping:
            return type_mapping[element_type_str]

        # Infer from content
        if "worksheet_name" in raw_element:
            return ElementType.WORKSHEET
        elif "filter_config" in raw_element:
            return ElementType.FILTER
        elif "parameter_config" in raw_element:
            return ElementType.PARAMETER
        elif "text_content" in raw_element:
            return ElementType.TEXT

        return ElementType.CUSTOM

    def _process_position(self, raw_position: Dict) -> Dict:
        """Process raw position data into Position format."""
        return {
            "x": max(0.0, min(1.0, float(raw_position.get("x", 0.0)))),
            "y": max(0.0, min(1.0, float(raw_position.get("y", 0.0)))),
            "width": max(0.0, min(1.0, float(raw_position.get("width", 0.1)))),
            "height": max(0.0, min(1.0, float(raw_position.get("height", 0.1)))),
            "z_index": int(raw_position.get("z_index", 0)),
        }

    def _process_style(self, raw_style: Dict) -> Dict:
        """Process raw style data into Style format."""
        return {
            "background_color": raw_style.get("background_color"),
            "border_color": raw_style.get("border_color"),
            "border_width": float(raw_style.get("border_width", 0)),
            "border_style": raw_style.get("border_style", "none"),
            "border_radius": float(raw_style.get("border_radius", 0)),
            "margin": float(raw_style.get("margin", 0)),
            "padding": float(raw_style.get("padding", 0)),
            "opacity": float(raw_style.get("opacity", 1.0)),
            "font_family": raw_style.get("font_family"),
            "font_size": raw_style.get("font_size"),
            "font_weight": raw_style.get("font_weight"),
            "text_color": raw_style.get("text_color"),
            "text_align": raw_style.get("text_align", "left"),
        }

    def _process_element_content(
        self, raw_element: Dict, element_type: ElementType
    ) -> Dict:
        """Process element content based on its type."""
        content_data = {}

        if element_type == ElementType.WORKSHEET:
            # For worksheet elements, we just store the reference
            # The actual WorksheetSchema will be populated by the migration engine
            content_data["worksheet"] = None  # Will be populated later

            # Store worksheet name for reference
            if "worksheet_name" in raw_element:
                content_data["custom_content"] = {
                    "worksheet_name": raw_element["worksheet_name"]
                }

        elif element_type == ElementType.FILTER:
            filter_config = raw_element.get("filter_config", {})
            content_data["filter_config"] = {
                "filter_type": filter_config.get("filter_type", "field_filter"),
                "field": filter_config.get("field", ""),
                "filter_values": filter_config.get("filter_values", []),
                "is_multiple_select": filter_config.get("is_multiple_select", True),
                "show_apply_button": filter_config.get("show_apply_button", False),
            }

        elif element_type == ElementType.PARAMETER:
            param_config = raw_element.get("parameter_config", {})
            content_data["parameter_config"] = {
                "parameter_name": param_config.get("parameter_name", ""),
                "data_type": param_config.get("data_type", "string"),
                "default_value": param_config.get("default_value"),
                "allowed_values": param_config.get("allowed_values", []),
                "control_type": param_config.get("control_type", "dropdown"),
            }

        elif element_type == ElementType.TEXT:
            content_data["text_content"] = raw_element.get("text_content", "")

        elif element_type == ElementType.IMAGE:
            image_config = raw_element.get("image_config", {})
            content_data["image_config"] = {
                "image_url": image_config.get("image_url", ""),
                "alt_text": image_config.get("alt_text", ""),
                "fit_mode": image_config.get("fit_mode", "fit"),
            }

        return content_data

    def _process_global_filters(self, raw_filters: List[Dict]) -> List[Dict]:
        """Process raw global filter data."""
        processed_filters = []

        for raw_filter in raw_filters:
            if not isinstance(raw_filter, dict):
                continue

            filter_data = {
                "name": raw_filter.get("name", ""),
                "title": raw_filter.get("title", raw_filter.get("name", "")),
                "field": raw_filter.get("field", ""),
                "filter_type": raw_filter.get("filter_type", "field_filter"),
                "default_value": raw_filter.get("default_value"),
                "explore": raw_filter.get("explore", ""),
                "applies_to": raw_filter.get("applies_to", []),
                "is_global": raw_filter.get("is_global", True),
            }

            processed_filters.append(filter_data)

        return processed_filters

    def _calculate_dashboard_confidence(
        self, data: Dict, elements: List[Dict]
    ) -> float:
        """Calculate confidence score for dashboard processing."""
        confidence = 0.7  # Base confidence

        # Boost for complete element data
        if elements and all(
            "element_id" in elem and "position" in elem for elem in elements
        ):
            confidence += 0.1

        # Boost for valid canvas size
        canvas = data.get("canvas_size", {})
        if canvas.get("width", 0) > 0 and canvas.get("height", 0) > 0:
            confidence += 0.1

        # Boost for worksheet elements (indicates real dashboard content)
        worksheet_elements = [
            e for e in elements if e.get("element_type") == "worksheet"
        ]
        if worksheet_elements:
            confidence += 0.1

        # Penalty for missing key data
        if not data.get("name"):
            confidence -= 0.2

        if not elements:
            confidence -= 0.3

        return max(0.0, min(1.0, confidence))

    def _clean_name(self, name: str) -> str:
        """Convert name to LookML-safe format."""
        import re

        # Convert to snake_case and remove special characters
        clean = re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())
        clean = re.sub(r"_+", "_", clean)  # Remove multiple underscores
        return clean.strip("_")
