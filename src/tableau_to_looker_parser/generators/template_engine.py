"""
Template engine for LookML generation using Jinja2.
"""

from pathlib import Path
from typing import Dict, Any, Optional
import logging

from jinja2 import Environment, FileSystemLoader, select_autoescape

logger = logging.getLogger(__name__)


class TemplateEngine:
    """Template engine for processing LookML templates using Jinja2."""

    def __init__(self, template_dir: Optional[str] = None):
        """
        Initialize the template engine.

        Args:
            template_dir: Directory containing template files. If None, uses default templates directory.
        """
        if template_dir is None:
            # Default to templates directory relative to this file
            current_dir = Path(__file__).parent
            template_dir = current_dir.parent / "templates"

        self.template_dir = Path(template_dir)

        # Create Jinja2 environment with secure settings
        self.env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )

        # Add custom filters
        self.env.filters["snake_case"] = self._snake_case_filter
        self.env.filters["clean_name"] = self._clean_name_filter

        logger.info(f"Template engine initialized with directory: {self.template_dir}")

    def _snake_case_filter(self, value: str) -> str:
        """Convert string to snake_case."""
        import re

        # Handle brackets and special characters
        value = re.sub(r"\[([^\]]+)\]", r"\1", value)  # Remove brackets
        value = re.sub(r"[^\w\s]", "_", value)  # Replace special chars with underscore
        value = re.sub(r"\s+", "_", value)  # Replace spaces with underscore
        value = re.sub(r"_+", "_", value)  # Replace multiple underscores with single
        value = value.strip(
            "_"
        ).lower()  # Remove leading/trailing underscores and lowercase

        return value

    def _clean_name_filter(self, value: str) -> str:
        """Clean field names for LookML."""
        # Remove brackets and clean up
        clean_value = value.replace("[", "").replace("]", "")
        return self._snake_case_filter(clean_value)

    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Render a template with the given context.

        Args:
            template_name: Name of the template file (e.g., 'connection.j2')
            context: Dictionary of variables to pass to the template

        Returns:
            Rendered template as string

        Raises:
            FileNotFoundError: If template file doesn't exist
            Exception: If template rendering fails
        """
        try:
            template = self.env.get_template(template_name)
            rendered = template.render(context)

            logger.debug(f"Successfully rendered template: {template_name}")
            return rendered

        except Exception as e:
            logger.error(f"Failed to render template {template_name}: {str(e)}")
            raise

    def render_string(self, template_string: str, context: Dict[str, Any]) -> str:
        """
        Render a template string with the given context.

        Args:
            template_string: Template content as string
            context: Dictionary of variables to pass to the template

        Returns:
            Rendered template as string
        """
        try:
            template = self.env.from_string(template_string)
            rendered = template.render(context)

            logger.debug("Successfully rendered template string")
            return rendered

        except Exception as e:
            logger.error(f"Failed to render template string: {str(e)}")
            raise

    def list_templates(self) -> list:
        """
        List all available templates.

        Returns:
            List of template filenames
        """
        if not self.template_dir.exists():
            return []

        templates = []
        for file_path in self.template_dir.glob("*.j2"):
            templates.append(file_path.name)

        return sorted(templates)

    def template_exists(self, template_name: str) -> bool:
        """
        Check if a template exists.

        Args:
            template_name: Name of the template file

        Returns:
            True if template exists, False otherwise
        """
        template_path = self.template_dir / template_name
        return template_path.exists()
