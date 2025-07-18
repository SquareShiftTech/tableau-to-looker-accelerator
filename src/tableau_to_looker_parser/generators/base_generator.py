"""
Base generator class for LookML file generation.
"""

from pathlib import Path
from typing import Optional
import logging

from .template_engine import TemplateEngine

logger = logging.getLogger(__name__)


class BaseGenerator:
    """Base class for all LookML generators."""

    def __init__(self, template_dir: Optional[str] = None):
        """
        Initialize the base generator.

        Args:
            template_dir: Directory containing template files. If None, uses default.
        """
        self.template_engine = TemplateEngine(template_dir)
        self.lookml_extension = ".lkml"

        logger.debug(f"Initialized {self.__class__.__name__}")

    def _clean_name(self, name: str) -> str:
        """Clean name for LookML."""
        return self.template_engine._clean_name_filter(name)

    def _ensure_output_dir(self, output_dir: str) -> Path:
        """Ensure output directory exists."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path

    def _write_file(self, content: str, file_path: Path) -> str:
        """Write content to file and return path."""
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"Generated {file_path}")
        return str(file_path)
