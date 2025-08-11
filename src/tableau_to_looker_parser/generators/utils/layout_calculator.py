"""
Layout calculation utilities for dashboard generation.

Handles conversion of Tableau positioning to LookML grid system with
support for different layout types and responsive design.
"""

from typing import Dict, Any
from ...models.dashboard_models import DashboardElement


class LayoutCalculator:
    """Utility class for calculating responsive layout positioning."""

    def __init__(self):
        """Initialize layout calculator."""
        self.grid_columns = 24  # LookML uses 24-column grid
        self.max_rows = 30  # Reasonable maximum for dashboards

    def calculate_responsive_layout(
        self, element: DashboardElement, migration_data: Dict[str, Any]
    ) -> Dict[str, int]:
        """
        Calculate responsive layout positioning based on Tableau layout type and element positioning.

        Handles different Tableau layout patterns:
        - layout-basic (free_form) → Direct coordinate translation
        - layout-flow horizontal (grid) → Optimized for horizontal flow
        - layout-flow vertical (newspaper) → Optimized for vertical stacking
        - Mixed flows (newspaper) → Complex grid positioning

        Args:
            element: Dashboard element with position information
            migration_data: Migration data containing dashboard context

        Returns:
            Dictionary with row, col, width, height for LookML
        """
        # Get dashboard layout type from migration data
        dashboard_info = self._find_dashboard_for_element(element, migration_data)
        layout_type = dashboard_info.get("layout_type", "free_form")

        # Base position from normalized coordinates
        base_layout = {
            "row": max(0, int(element.position.y * 20)),
            "col": max(0, int(element.position.x * self.grid_columns)),
            "width": max(1, int(element.position.width * self.grid_columns)),
            "height": max(1, int(element.position.height * 20)),
        }

        # Apply layout-specific optimizations
        if layout_type == "newspaper":
            return self._optimize_for_newspaper_layout(base_layout, element)
        elif layout_type == "grid":
            return self._optimize_for_grid_layout(base_layout, element)
        else:
            return self._optimize_for_freeform_layout(base_layout, element)

    def _find_dashboard_for_element(
        self, element: DashboardElement, migration_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Find dashboard containing the given element."""
        dashboards = migration_data.get("dashboards", [])

        for dashboard in dashboards:
            elements = dashboard.get("elements", [])
            for elem in elements:
                if elem.get("element_id") == element.element_id:
                    return dashboard

        return {}

    def _optimize_for_newspaper_layout(
        self, layout: Dict[str, int], element: DashboardElement
    ) -> Dict[str, int]:
        """Optimize positioning for newspaper-style layout (vertical stacking)."""
        optimized = layout.copy()

        # Ensure minimum readable width
        if optimized["width"] < 6:
            optimized["width"] = 6

        # Ensure reasonable height for charts
        if optimized["height"] < 4:
            optimized["height"] = 4

        # Snap to newspaper-friendly grid (multiples of 4)
        optimized["col"] = (optimized["col"] // 4) * 4
        optimized["width"] = max(4, (optimized["width"] // 4) * 4)

        return optimized

    def _optimize_for_grid_layout(
        self, layout: Dict[str, int], element: DashboardElement
    ) -> Dict[str, int]:
        """Optimize positioning for grid layout (horizontal flow)."""
        optimized = layout.copy()

        # Ensure elements fit well in horizontal flow
        if optimized["width"] < 3:
            optimized["width"] = 3

        # Align to grid boundaries (multiples of 3 for 24-column grid)
        optimized["col"] = (optimized["col"] // 3) * 3
        optimized["width"] = max(3, (optimized["width"] // 3) * 3)

        # Consistent heights for horizontal alignment
        if optimized["height"] < 3:
            optimized["height"] = 3

        return optimized

    def _optimize_for_freeform_layout(
        self, layout: Dict[str, int], element: DashboardElement
    ) -> Dict[str, int]:
        """Optimize positioning for free-form layout (absolute positioning)."""
        optimized = layout.copy()

        # Ensure minimum viable sizes
        optimized["width"] = max(1, optimized["width"])
        optimized["height"] = max(1, optimized["height"])

        # Ensure elements don't go off-screen
        if optimized["col"] + optimized["width"] > self.grid_columns:
            optimized["col"] = max(0, self.grid_columns - optimized["width"])

        if optimized["row"] + optimized["height"] > self.max_rows:
            optimized["row"] = max(0, self.max_rows - optimized["height"])

        return optimized

    def normalize_tableau_coordinates(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        canvas_width: int = 1000,
        canvas_height: int = 800,
    ) -> Dict[str, float]:
        """
        Normalize Tableau coordinates (100k scale) to 0-1 scale.

        Args:
            x, y, width, height: Tableau coordinates (typically in 100k scale)
            canvas_width, canvas_height: Dashboard canvas dimensions

        Returns:
            Dictionary with normalized coordinates
        """
        # Tableau uses 100k scale internally
        scale_factor = 100000

        return {
            "x": x / scale_factor,
            "y": y / scale_factor,
            "width": width / scale_factor,
            "height": height / scale_factor,
        }

    def calculate_element_overlap(
        self, element1: Dict[str, int], element2: Dict[str, int]
    ) -> float:
        """
        Calculate overlap percentage between two elements.

        Args:
            element1, element2: Layout dictionaries with row, col, width, height

        Returns:
            Overlap percentage (0.0 to 1.0)
        """
        # Calculate boundaries
        e1_left = element1["col"]
        e1_right = element1["col"] + element1["width"]
        e1_top = element1["row"]
        e1_bottom = element1["row"] + element1["height"]

        e2_left = element2["col"]
        e2_right = element2["col"] + element2["width"]
        e2_top = element2["row"]
        e2_bottom = element2["row"] + element2["height"]

        # Calculate overlap
        overlap_left = max(e1_left, e2_left)
        overlap_right = min(e1_right, e2_right)
        overlap_top = max(e1_top, e2_top)
        overlap_bottom = min(e1_bottom, e2_bottom)

        # Check if there's any overlap
        if overlap_left >= overlap_right or overlap_top >= overlap_bottom:
            return 0.0

        # Calculate overlap area
        overlap_area = (overlap_right - overlap_left) * (overlap_bottom - overlap_top)

        # Calculate total area of both elements
        e1_area = element1["width"] * element1["height"]
        e2_area = element2["width"] * element2["height"]

        # Return overlap as percentage of smaller element
        min_area = min(e1_area, e2_area)
        if min_area == 0:
            return 0.0

        return overlap_area / min_area

    def suggest_layout_improvements(self, elements: list) -> Dict[str, Any]:
        """
        Analyze layout and suggest improvements.

        Args:
            elements: List of element layout dictionaries

        Returns:
            Dictionary with improvement suggestions
        """
        suggestions = {
            "overlapping_elements": [],
            "off_screen_elements": [],
            "size_warnings": [],
            "alignment_suggestions": [],
        }

        for i, element in enumerate(elements):
            # Check for off-screen elements
            if (
                element["col"] + element["width"] > self.grid_columns
                or element["row"] + element["height"] > self.max_rows
            ):
                suggestions["off_screen_elements"].append(i)

            # Check for size warnings
            if element["width"] < 2 or element["height"] < 2:
                suggestions["size_warnings"].append(i)

            # Check for overlaps with other elements
            for j, other_element in enumerate(elements[i + 1 :], i + 1):
                overlap = self.calculate_element_overlap(element, other_element)
                if overlap > 0.1:  # More than 10% overlap
                    suggestions["overlapping_elements"].append((i, j, overlap))

        return suggestions

    def calculate_looker_position(
        self,
        element: DashboardElement,
        migration_data: Dict[str, Any],
        height_based_rows: Dict[str, int] = None,
    ) -> Dict[str, int]:
        """
        Calculate clean Looker-native positioning from dashboard element.

        Simplified version for Looker-native dashboards without ECharts complexity.

        Args:
            element: Dashboard element with position information
            migration_data: Migration data containing dashboard context

        Returns:
            Dictionary with row, col, width, height for Looker LookML
        """
        # Use height-based row if provided, otherwise fall back to y-coordinate
        if height_based_rows and element.element_id in height_based_rows:
            calculated_row = height_based_rows[element.element_id]
        else:
            calculated_row = max(0, int(element.position.y * 20))

        position = {
            "row": calculated_row,
            "col": max(0, int(element.position.x * self.grid_columns)),
            "width": max(1, int(element.position.width * self.grid_columns)),
            "height": max(1, int(element.position.height * 20)),
        }

        # Apply manual dashboard minimum sizes: width=6, height=5 for donut charts
        position["width"] = max(6, position["width"])  # Match manual dashboard minimum

        position["height"] = max(
            5, position["height"]
        )  # Match manual dashboard minimum

        return position

    def calculate_height_based_rows(self, elements: list) -> Dict[str, int]:
        """
        Calculate proper row positioning based on element heights to avoid overlapping.

        Args:
            elements: List of dashboard elements with position information

        Returns:
            Dictionary mapping element_id to calculated row position
        """
        if not elements:
            return {}

        # Group elements by their y-coordinate (same visual row)
        y_tolerance = 0.05  # Elements within 5% y-difference are considered same row
        row_groups = []

        for element in elements:
            y_pos = element.position.y

            # Find existing group with similar y-coordinate
            found_group = False
            for group in row_groups:
                if abs(group[0].position.y - y_pos) <= y_tolerance:
                    group.append(element)
                    found_group = True
                    break

            # Create new group if no match found
            if not found_group:
                row_groups.append([element])

        # Sort groups by y-coordinate
        row_groups.sort(key=lambda group: group[0].position.y)

        # Calculate cumulative row positions based on heights
        element_rows = {}
        current_row = 0

        for group in row_groups:
            # Calculate heights for all elements in this group
            group_heights = []
            for elem in group:
                calculated_height = max(5, int(elem.position.height * 20))
                group_heights.append(calculated_height)

            # All elements in same group get same row value
            max_height_in_group = max(group_heights)

            for elem in group:
                element_rows[elem.element_id] = current_row

            # Next row starts after this group's max height + spacing
            current_row += max_height_in_group + 2  # +2 for proper spacing

        return element_rows

    def calculate_standardized_widths(self, elements: list) -> Dict[str, int]:
        """
        Standardize all rows to full width (24 columns) by calculating element widths per row.

        Args:
            elements: List of dashboard elements with position information

        Returns:
            Dictionary mapping element_id to standardized width
        """
        if not elements:
            return {}

        # Group elements by their y-coordinate (same visual row)
        y_tolerance = 0.05  # Elements within 5% y-difference are considered same row
        row_groups = []

        for element in elements:
            y_pos = element.position.y

            # Find existing group with similar y-coordinate
            found_group = False
            for group in row_groups:
                if abs(group[0].position.y - y_pos) <= y_tolerance:
                    group.append(element)
                    found_group = True
                    break

            # Create new group if no match found
            if not found_group:
                row_groups.append([element])

        element_widths = {}

        for group in row_groups:
            num_elements = len(group)

            if num_elements == 1:
                # Single element gets full width
                element_widths[group[0].element_id] = 24
            else:
                # Multiple elements share width equally
                width_per_element = 24 // num_elements
                remaining_width = 24 % num_elements

                # Sort elements by x position for consistent assignment
                group.sort(key=lambda elem: elem.position.x)

                for i, element in enumerate(group):
                    # Distribute remaining width to first few elements
                    extra_width = 1 if i < remaining_width else 0
                    element_widths[element.element_id] = width_per_element + extra_width

        return element_widths
