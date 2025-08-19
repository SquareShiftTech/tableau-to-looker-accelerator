"""
TableauStyleExtractor - Extract styling information from Tableau XML.

This class extracts color mappings, fonts, and formatting from Tableau workbooks
and provides structured styling data that can be used by chart generators.
"""

import logging
from typing import Dict, Any, Optional
from xml.etree.ElementTree import Element

logger = logging.getLogger(__name__)


class TableauStyleExtractor:
    """Extract styling information from Tableau XML without breaking existing functionality."""

    def __init__(self):
        """Initialize the style extractor."""
        self.logger = logging.getLogger(__name__)

    def extract_worksheet_styling(
        self, worksheet: Element, worksheet_name: str
    ) -> Dict[str, Any]:
        """
        Extract styling information for a single worksheet.

        Args:
            worksheet: Worksheet XML element
            worksheet_name: Name of the worksheet

        Returns:
            Dict containing styling information for this worksheet
        """
        try:
            styling_data = {
                "title_style": self._extract_title_style(worksheet),
                "color_mappings": self._extract_color_mappings(worksheet),
                "chart_detection": self._detect_chart_type(worksheet),
                "custom_tooltip": self._extract_custom_tooltip(worksheet),
                "table_style": self._extract_table_style(worksheet),
            }

            # NEW: Extract datasource-level field-specific color mappings
            field_color_mappings = self._extract_datasource_color_mappings(worksheet)
            if field_color_mappings:
                styling_data.update(field_color_mappings)

            # Only return non-empty styling data
            filtered_styling = {k: v for k, v in styling_data.items() if v}
            return filtered_styling

        except Exception as e:
            self.logger.warning(
                f"Failed to extract styling for worksheet {worksheet_name}: {str(e)}"
            )
            return {}

    def extract_all_styling(self, root: Element) -> Dict[str, Any]:
        """
        Extract all styling information from Tableau XML root.

        Args:
            root: Root element of the Tableau workbook XML

        Returns:
            Dict containing all extracted styling information
        """
        try:
            styling_data = {
                "worksheet_styles": self._extract_worksheet_styles(root),
                "global_styles": self._extract_global_styles(root),
                "color_palettes": self._extract_color_palettes(root),
                "extraction_successful": True,
            }

            self.logger.info(
                f"Successfully extracted styling for {len(styling_data['worksheet_styles'])} worksheets"
            )
            return styling_data

        except Exception as e:
            self.logger.error(f"Failed to extract styling: {str(e)}")
            return {
                "worksheet_styles": {},
                "global_styles": {},
                "color_palettes": {},
                "extraction_successful": False,
                "error": str(e),
            }

    def _extract_worksheet_styles(self, root: Element) -> Dict[str, Dict[str, Any]]:
        """Extract styling information for each worksheet."""
        worksheet_styles = {}

        for worksheet in root.findall(".//worksheet"):
            worksheet_name = worksheet.get("name")
            if not worksheet_name:
                continue

            try:
                worksheet_style = {
                    "title_style": self._extract_title_style(worksheet),
                    "color_mappings": self._extract_color_mappings(worksheet),
                    "chart_detection": self._detect_chart_type(worksheet),
                    "custom_tooltip": self._extract_custom_tooltip(worksheet),
                    "table_style": self._extract_table_style(worksheet),
                }

                # Only add if we found some styling information
                if any(worksheet_style.values()):
                    worksheet_styles[worksheet_name] = worksheet_style
                    self.logger.debug(
                        f"Extracted styling for worksheet: {worksheet_name}"
                    )

            except Exception as e:
                self.logger.warning(
                    f"Failed to extract styling for worksheet {worksheet_name}: {str(e)}"
                )
                continue

        return worksheet_styles

    def _extract_title_style(self, worksheet: Element) -> Optional[Dict[str, Any]]:
        """Extract title formatting from worksheet."""
        title_elem = worksheet.find(".//title/formatted-text/run")
        if title_elem is None:
            return None

        style = {}

        # Extract formatting attributes
        if title_elem.get("bold") == "true":
            style["bold"] = True
        if title_elem.get("fontalignment"):
            style["alignment"] = title_elem.get("fontalignment")
        if title_elem.get("fontsize"):
            style["font_size"] = title_elem.get("fontsize")
        if title_elem.get("fontcolor"):
            style["color"] = title_elem.get("fontcolor")

        # Extract title text
        if title_elem.text:
            style["text"] = title_elem.text.strip()

        return style if style else None

    def _extract_color_mappings(self, worksheet: Element) -> Dict[str, Any]:
        """Extract color mappings from worksheet encodings and global datasource styles."""
        color_data = {}

        # First, look for color encodings in worksheet panes
        for encoding in worksheet.findall(".//encoding[@attr='color']"):
            field = encoding.get("field", "") or encoding.get("column", "")
            encoding_type = encoding.get("type", "")

            if not field:
                continue

            # Extract field name from complex Tableau field reference
            field_name = self._extract_field_name(field)

            # Check if this is categorical (discrete) or continuous mapping
            if encoding_type == "interpolated":
                # Continuous color mapping (heat maps)
                color_data = {
                    "type": "continuous",
                    "field": field_name,
                    "encoding_type": "interpolated",
                    "full_field_reference": field,
                }
            else:
                # Discrete color mappings (like New/Upgrade)
                mappings = {}
                for map_elem in encoding.findall("map"):
                    color = map_elem.get("to")
                    bucket = map_elem.find("bucket")

                    if bucket is not None and bucket.text and color:
                        value = bucket.text.strip('"')
                        mappings[value] = color

                if mappings:
                    color_data = {
                        "type": "categorical",
                        "field": field_name,
                        "mappings": mappings,
                        "full_field_reference": field,
                    }

        return color_data

    def _extract_datasource_color_mappings(self, worksheet: Element) -> Dict[str, Any]:
        """
        Extract field-specific categorical color mappings from datasource style rules,
        organized by datasource to handle multiple datasources correctly.

        Args:
            worksheet: Worksheet XML element

        Returns:
            Dict containing datasource-specific field color mappings in format:
            {"field_color_mappings": {"datasource_name": {"field_name": {"type": "categorical", "mappings": {...}}}}}
        """
        try:
            # Get the root document to access datasource styles
            root = worksheet.getroottree().getroot()

            # Group color mappings by datasource
            datasource_color_mappings = {}

            # Look for color mappings in datasource style rules
            for datasource in root.findall(".//datasource"):
                datasource_name = datasource.get("name", "unknown")
                datasource_caption = datasource.get("caption", datasource_name)

                # Initialize datasource entry if not exists
                if datasource_name not in datasource_color_mappings:
                    datasource_color_mappings[datasource_name] = {
                        "name": datasource_name,
                        "caption": datasource_caption,
                        "fields": {},
                    }

                # Extract color mappings for this specific datasource
                for style_rule in datasource.findall(".//style-rule[@element='mark']"):
                    for encoding in style_rule.findall("encoding[@attr='color']"):
                        field = encoding.get("field", "")
                        encoding_type = encoding.get("type", "")

                        if not field or encoding_type != "palette":
                            continue

                        # Extract field name from the full reference
                        field_name = self._extract_field_name(field)

                        # Look for discrete color mappings
                        mappings = {}
                        for map_elem in encoding.findall("map"):
                            color = map_elem.get("to")
                            bucket = map_elem.find("bucket")

                            if bucket is not None and bucket.text and color:
                                value = bucket.text.strip('"')
                                mappings[value] = color

                        if mappings:
                            datasource_color_mappings[datasource_name]["fields"][
                                field_name
                            ] = {
                                "type": "categorical",
                                "field": field_name,
                                "mappings": mappings,
                                "full_field_reference": field,
                                "datasource": datasource_name,
                            }
                            self.logger.debug(
                                f"Found color mappings for field {field_name} in datasource {datasource_name}: {list(mappings.keys())}"
                            )

            # Return datasource-organized field mappings
            return (
                {"field_color_mappings": datasource_color_mappings}
                if datasource_color_mappings
                else {}
            )

        except Exception as e:
            self.logger.warning(
                f"Failed to extract datasource color mappings: {str(e)}"
            )
            return {}

    def _extract_datasource_color_mappings_original(
        self, worksheet: Element
    ) -> Dict[str, Any]:
        """Extract color mappings from global datasource style rules that might apply to this worksheet."""

        # Get the root document to access datasource styles
        root = worksheet.getroottree().getroot()

        # First, check if this worksheet actually uses fields containing the color-coded values
        has_color_coded_fields = self._worksheet_contains_color_coded_fields(worksheet)
        if not has_color_coded_fields:
            self.logger.debug(
                "Worksheet does not contain color-coded fields - skipping global color mappings"
            )
            return {}

        # Collect all potential color mappings and prioritize specific ones
        potential_mappings = []

        # Look for color mappings in datasource style rules
        for datasource in root.findall(".//datasource"):
            for style_rule in datasource.findall(".//style-rule[@element='mark']"):
                for encoding in style_rule.findall("encoding[@attr='color']"):
                    field = encoding.get("field", "")
                    encoding_type = encoding.get("type", "")

                    if not field or encoding_type != "palette":
                        continue

                    # Extract field name
                    field_name = self._extract_field_name(field)

                    # Look for discrete color mappings
                    mappings = {}
                    for map_elem in encoding.findall("map"):
                        color = map_elem.get("to")
                        bucket = map_elem.find("bucket")

                        if bucket is not None and bucket.text and color:
                            value = bucket.text.strip('"')
                            mappings[value] = color

                    if mappings:
                        # Calculate priority - specific fields get higher priority
                        priority = self._calculate_field_priority(field, mappings)

                        potential_mappings.append(
                            {
                                "type": "categorical",
                                "field": field_name,
                                "mappings": mappings,
                                "full_field_reference": field,
                                "source": "global_datasource_style",
                                "priority": priority,
                            }
                        )

        # Sort by priority and return the best match
        if potential_mappings:
            potential_mappings.sort(key=lambda x: x["priority"], reverse=True)
            best_mapping = potential_mappings[0]
            # Remove priority before returning
            del best_mapping["priority"]
            return best_mapping

        return {}

    def _worksheet_contains_color_coded_fields(self, worksheet: Element) -> bool:
        """Check if worksheet contains fields that have color-coded values like New/Upgrade."""

        # Look for the specific calculated field that contains New/Upgrade values
        calculation_field_pattern = "calculation_5910989867950081"

        # Check in panes, columns, rows, and other field references
        field_references = (
            worksheet.findall(".//panes//pane//view//datasource-dependencies//column")
            + worksheet.findall(".//table//panes//pane//view//plot//columns")
            + worksheet.findall(".//table//panes//pane//view//plot//rows")
            + worksheet.findall(".//table//view//datasource-dependencies//column")
        )

        for field_elem in field_references:
            field_name = field_elem.get("name", "")
            if calculation_field_pattern in field_name:
                self.logger.debug(f"Found color-coded field: {field_name}")
                return True

        # Also check if worksheet uses fields with explicit color mappings in its encoding
        for encoding in worksheet.findall(".//encoding[@attr='color']"):
            field = encoding.get("field", "")
            if calculation_field_pattern in field:
                self.logger.debug(f"Found color encoding field: {field}")
                return True

        return False

    def _calculate_field_priority(self, field: str, mappings: Dict[str, str]) -> int:
        """Calculate priority for field mappings - higher is better."""
        priority = 0

        # Prioritize specific calculated fields over generic ones
        if "Calculation_5910989867950081" in field:  # Sale Type field
            priority += 100

        # Prioritize fields with meaningful value names (New, Upgrade, etc.)
        meaningful_values = {"New", "Upgrade", "Yes", "No", "True", "False"}
        if any(value in meaningful_values for value in mappings.keys()):
            priority += 50

        # Deprioritize generic measure names
        if ":Measure Names" in field or "attr:" in field or len(mappings) > 10:
            priority -= 30

        # Small bonus for fewer mappings (more specific)
        priority += max(0, 10 - len(mappings))

        return priority

    def _field_used_in_worksheet(self, field_name: str, worksheet: Element) -> bool:
        """Check if a field is used in the given worksheet."""
        # Look for the field in worksheet's datasource dependencies
        for column in worksheet.findall(".//column"):
            name = column.get("name", "")
            caption = column.get("caption", "")

            # Check various field name patterns
            if (
                field_name in name
                or field_name in caption
                or field_name.replace("_", " ").lower() in caption.lower()
                or "Calculation_5910989867950081" in name
            ):  # Specific to Sale Type field
                return True

        return False

    def _extract_field_name(self, field_reference: str) -> str:
        """Extract clean field name from Tableau field reference."""
        # Handle field references like:
        # [federated.1fc6jd010l1f0m19s90ze0noolhe].[none:Calculation_5910989867950081:nk]
        # Should return: Calculation_5910989867950081 or a cleaner name

        if not field_reference:
            return ""

        # Extract the part after the last dot and remove brackets/prefixes
        try:
            if "].[" in field_reference:
                field_part = field_reference.split("].[")[-1].rstrip("]")
            else:
                # Simple field reference, just clean it up
                field_part = field_reference.strip("[]")

            # Remove prefixes like 'none:', 'sum:', etc. and suffixes like ':nk'
            if ":" in field_part:
                parts = field_part.split(":")
                # Take the middle part (field name), skip prefix and suffix
                if len(parts) >= 2:
                    return parts[1]
                return parts[0]
            return field_part
        except Exception:
            return field_reference

    def _detect_chart_type(self, worksheet: Element) -> Dict[str, Any]:
        """Detect chart type based on worksheet structure."""
        chart_info = {}

        # Look for mark class to determine chart type
        mark_elem = worksheet.find(".//mark")
        if mark_elem is not None:
            mark_class = mark_elem.get("class", "")
            chart_info["tableau_mark_type"] = mark_class

            # Map to likely Looker equivalent
            looker_type_mapping = {
                "Pie": "looker_donut_multiples",
                "Square": "looker_grid",
                "Bar": "looker_bar",
                "Line": "looker_line",
                "Circle": "looker_scatter",
            }
            chart_info["suggested_looker_type"] = looker_type_mapping.get(
                mark_class, "table"
            )

        # Check for pivot structure (indicates table)
        if (
            worksheet.find(".//cols") is not None
            or worksheet.find(".//rows") is not None
        ):
            chart_info["has_pivot_structure"] = True

        return chart_info

    def _extract_custom_tooltip(self, worksheet: Element) -> Optional[Dict[str, Any]]:
        """Extract custom tooltip formatting."""
        tooltip_elem = worksheet.find(".//customized-tooltip/formatted-text")
        if tooltip_elem is None:
            return None

        tooltip_data = {"has_custom_tooltip": True, "formatting_elements": []}

        # Extract formatting runs
        for run in tooltip_elem.findall("run"):
            run_data = {}
            if run.get("fontcolor"):
                run_data["color"] = run.get("fontcolor")
            if run.get("bold") == "true":
                run_data["bold"] = True
            if run.text:
                run_data["text"] = run.text

            if run_data:
                tooltip_data["formatting_elements"].append(run_data)

        return tooltip_data if tooltip_data["formatting_elements"] else None

    def _extract_table_style(self, worksheet: Element) -> Optional[Dict[str, Any]]:
        """Extract table-specific styling from worksheet style rules."""
        worksheet_name = worksheet.get("name", "unknown")
        style_section = worksheet.find(".//style")
        if style_section is None:
            self.logger.debug(f"No style section found for worksheet: {worksheet_name}")
            return None

        self.logger.debug(f"Found style section for worksheet: {worksheet_name}")
        # Debug: count style rules
        style_rules = style_section.findall(".//style-rule")
        self.logger.debug(f"Found {len(style_rules)} style rules in {worksheet_name}")
        for rule in style_rules:
            element_type = rule.get("element", "unknown")
            self.logger.debug(f"  - Style rule for element: {element_type}")

        table_style = {}

        # Extract header styling (background colors, borders)
        header_rule = style_section.find(".//style-rule[@element='header']")
        if header_rule is not None:
            header_styles = {}

            # Header background colors
            for format_elem in header_rule.findall(
                ".//format[@attr='background-color']"
            ):
                scope = format_elem.get("scope", "all")
                color = format_elem.get("value")
                if color:
                    if scope == "cols":
                        header_styles["column_header_bg"] = color
                    elif scope == "rows":
                        header_styles["row_header_bg"] = color
                    else:
                        header_styles["header_bg"] = color

            # Header borders
            for format_elem in header_rule.findall(".//format[@attr='border-style']"):
                scope = format_elem.get("scope", "all")
                style_val = format_elem.get("value")
                if style_val:
                    header_styles[f"{scope}_border_style"] = style_val

            for format_elem in header_rule.findall(".//format[@attr='border-width']"):
                scope = format_elem.get("scope", "all")
                width = format_elem.get("value")
                if width:
                    header_styles[f"{scope}_border_width"] = width

            if header_styles:
                table_style["header"] = header_styles

        # Extract label styling (header text colors, alignment)
        label_rule = style_section.find(".//style-rule[@element='label']")
        if label_rule is not None:
            label_styles = {}

            # Text colors in headers
            for format_elem in label_rule.findall(".//format[@attr='color']"):
                scope = format_elem.get("scope", "all")
                color = format_elem.get("value")
                if color:
                    if scope == "cols":
                        label_styles["column_text_color"] = color
                    elif scope == "rows":
                        label_styles["row_text_color"] = color
                    else:
                        label_styles["text_color"] = color

            # Text alignment
            align_elem = label_rule.find(".//format[@attr='text-align']")
            if align_elem is not None:
                label_styles["text_align"] = align_elem.get("value")

            if label_styles:
                table_style["labels"] = label_styles

        # Extract cell styling (data cell colors, alignment)
        cell_rule = style_section.find(".//style-rule[@element='cell']")
        if cell_rule is not None:
            cell_styles = {}

            # Cell background color
            bg_elem = cell_rule.find(".//format[@attr='background-color']")
            if bg_elem is not None:
                cell_styles["background_color"] = bg_elem.get("value")

            # Cell text alignment
            align_elem = cell_rule.find(".//format[@attr='text-align']")
            if align_elem is not None:
                cell_styles["text_align"] = align_elem.get("value")

            # Cell vertical alignment
            valign_elem = cell_rule.find(".//format[@attr='vertical-align']")
            if valign_elem is not None:
                cell_styles["vertical_align"] = valign_elem.get("value")

            if cell_styles:
                table_style["cells"] = cell_styles

        # Extract table background
        table_rule = style_section.find(".//style-rule[@element='table']")
        if table_rule is not None:
            bg_elem = table_rule.find(".//format[@attr='background-color']")
            if bg_elem is not None:
                table_style["table_background"] = bg_elem.get("value")

        # Extract mark styling for cell values (ash color)
        mark_rule = style_section.find(".//style-rule[@element='mark']")
        if mark_rule is not None:
            mark_styles = {}

            # Color encodings for data values
            for encoding in mark_rule.findall(".//encoding[@attr='color']"):
                palette = encoding.get("palette", "")
                enc_type = encoding.get("type", "")

                if palette and enc_type:
                    mark_styles["value_color_palette"] = palette
                    mark_styles["value_color_type"] = enc_type

            if mark_styles:
                table_style["data_values"] = mark_styles

        return table_style if table_style else None

    def _extract_global_styles(self, root: Element) -> Dict[str, Any]:
        """Extract global styling information."""
        global_styles = {}

        # Extract global font family
        font_elem = root.find(
            ".//style-rule[@element='all']/format[@attr='font-family']"
        )
        if font_elem is not None:
            global_styles["font_family"] = font_elem.get("value")

        # Extract workbook-level preferences
        preferences = root.find(".//preferences")
        if preferences is not None:
            global_styles["preferences_found"] = True

        return global_styles

    def _extract_color_palettes(self, root: Element) -> Dict[str, Dict[str, Any]]:
        """Extract custom color palettes."""
        palettes = {}

        for palette in root.findall(".//color-palette"):
            palette_name = palette.get("name")
            palette_type = palette.get("type", "regular")

            if palette_name:
                colors = []
                for color in palette.findall("color"):
                    if color.text:
                        colors.append(color.text.strip())

                if colors:
                    palettes[palette_name] = {
                        "colors": colors,
                        "type": palette_type,
                        "custom": palette.get("custom", "false") == "true",
                    }

        return palettes

    def _extract_field_name(self, field_reference: str) -> str:
        """
        Extract clean field name from Tableau field reference.

        Examples:
            '[none:IS_PREORDER:nk]' -> 'IS_PREORDER'
            '[none:Calculation_5910989867950081:nk]' -> 'Calculation_5910989867950081'
            '[federated.xxx].[none:mkt:nk]' -> 'mkt'
        """
        if not field_reference:
            return ""

        # Handle federated references: [datasource].[field] -> field
        if "].[" in field_reference:
            field_reference = field_reference.split("].[", 1)[1]

        # Extract from bracketed reference: [none:FIELD:nk] -> FIELD
        if field_reference.startswith("[") and field_reference.endswith("]"):
            inner = field_reference[1:-1]
            parts = inner.split(":")
            if len(parts) >= 2:
                return parts[1]  # Get the middle part (field name)

        # Fallback: return as-is
        return field_reference
