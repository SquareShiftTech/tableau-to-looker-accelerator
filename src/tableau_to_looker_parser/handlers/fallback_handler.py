from typing import Any, Dict, List
from xml.etree.ElementTree import Element

from tableau_to_looker_parser.handlers.base_handler import BaseHandler


class FallbackHandler(BaseHandler):
    """Handler for unknown or unsupported Tableau elements.

    Handles:
    - Unknown element types
    - Elements not matched by other handlers
    - Elements requiring manual review
    - Basic element information extraction
    - Tracking of unknown elements for analysis
    """

    def __init__(self):
        """Initialize the handler with tracking storage."""
        self._unknown_elements = {}  # track frequency of unknown elements

    def can_handle(self, data: Any) -> float:
        """Determine if this handler can process the element.

        Args:
            element: XML element to check

        Returns:
            float: Always returns 0.1 for valid elements, 0.0 for invalid
        """
        if not isinstance(data, Element):
            return 0.0

        # Always handle any element with low confidence
        return 0.1

    def extract(self, element: Any) -> Dict:
        """Extract basic information from element.

        Args:
            element: XML element to process

        Returns:
            Dict: Raw element data

        Raises:
            ValueError: If element cannot be processed
        """
        if not isinstance(element, Element):
            raise ValueError("Expected XML Element")

        # Get basic attributes
        data = {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "text": element.text.strip() if element.text else None,
            "children": [],
            "review_notes": [],
        }

        # Track unknown element
        key = (element.tag, frozenset(element.attrib.items()))
        self._unknown_elements[key] = self._unknown_elements.get(key, 0) + 1

        # Note unknown element type and frequency
        data["review_notes"].append(
            f"Unknown element type: {element.tag} (seen {self._unknown_elements[key]} times)"
        )

        # Analyze attributes
        if element.attrib:
            data["review_notes"].append(
                f"Contains attributes: {', '.join(sorted(element.attrib.keys()))}"
            )

        # Analyze structure
        if len(element) > 0:
            child_tags = [child.tag for child in element]
            unique_tags = sorted(set(child_tags))
            data["review_notes"].append(
                f"Child element types: {', '.join(unique_tags)}"
            )
            if len(child_tags) != len(unique_tags):
                data["review_notes"].append("Contains repeated child element types")

        # Note if element has both text and children
        if data["text"] and len(element) > 0:
            data["review_notes"].append("Contains both text content and child elements")

        # Extract child elements
        for child in element:
            child_data = {
                "tag": child.tag,
                "attributes": dict(child.attrib),
                "text": child.text.strip() if child.text else None,
                "children": [],  # Only go one level deep
            }
            data["children"].append(child_data)

        # Add notes about content
        if data["text"]:
            data["review_notes"].append("Text content present")
        if data["children"]:
            data["review_notes"].append(
                f"Contains {len(data['children'])} child elements"
            )

        return data

    def convert_to_json(self, data: Dict) -> Dict:
        """Convert raw element data to schema-compliant JSON.

        Args:
            data: Raw element data from extract()

        Returns:
            Dict: Schema-compliant JSON data
        """
        # Build JSON structure
        json_data = {
            "element_type": data["tag"],
            "attributes": data["attributes"],
            "text_content": data["text"],
            "child_elements": [
                {
                    "element_type": child["tag"],
                    "attributes": child["attributes"],
                    "text_content": child["text"],
                }
                for child in data["children"]
            ],
            "review_required": True,
            "review_notes": data["review_notes"],
            "_metadata": {"handler": "FallbackHandler", "confidence": 0.1},
        }

        return json_data

    def get_unknown_elements_stats(self) -> Dict[str, List[Dict]]:
        """Get statistics about encountered unknown elements.

        Returns:
            Dict: Statistics about unknown elements, grouped by tag name
        """
        stats = {}
        for (tag, attrs), count in self._unknown_elements.items():
            if tag not in stats:
                stats[tag] = []
            stats[tag].append({"attributes": dict(attrs), "count": count})
        return stats
