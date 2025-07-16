"""Tableau to LookML Migration Library.

This library provides a clean, extensible architecture for migrating
Tableau workbooks (.twb/.twbx) to LookML format.
"""

from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.core.xml_parser import TableauXMLParser
from tableau_to_looker_parser.core.plugin_registry import PluginRegistry
from tableau_to_looker_parser.handlers.base_handler import BaseHandler

# Import all handlers for easy access
from tableau_to_looker_parser.handlers.dimension_handler import DimensionHandler
from tableau_to_looker_parser.handlers.measure_handler import MeasureHandler
from tableau_to_looker_parser.handlers.connection_handler import ConnectionHandler
from tableau_to_looker_parser.handlers.relationship_handler import RelationshipHandler
from tableau_to_looker_parser.handlers.parameter_handler import ParameterHandler
from tableau_to_looker_parser.handlers.fallback_handler import FallbackHandler

# Version
__version__ = "0.1.0"

# Public API
__all__ = [
    # Core components
    "MigrationEngine",
    "TableauXMLParser",
    "PluginRegistry",
    "BaseHandler",
    # Handlers
    "DimensionHandler",
    "MeasureHandler",
    "ConnectionHandler",
    "RelationshipHandler",
    "ParameterHandler",
    "FallbackHandler",
    # Version
    "__version__",
]
