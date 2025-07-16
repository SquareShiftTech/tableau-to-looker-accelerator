from typing import Dict, List, Optional
from tableau_to_looker_parser.handlers.base_handler import BaseHandler


class PluginRegistry:
    """Central registry for all Tableau element handlers.

    Manages handler registration and routing of elements to appropriate handlers.
    Uses priority-based handler selection with fallback support.
    """

    def __init__(self):
        # Dict of priority -> list of handlers
        self._handlers: Dict[int, List[BaseHandler]] = {}
        # Fallback handlers for unknown elements
        self._fallback_handlers: List[BaseHandler] = []

    def register_handler(self, handler: BaseHandler, priority: int = 100) -> None:
        """Register a new handler with given priority.

        Args:
            handler: Handler instance to register
            priority: Priority level (lower number = higher priority)

        Raises:
            ValueError: If handler is not a BaseHandler instance
        """
        if not isinstance(handler, BaseHandler):
            raise ValueError("Handler must be an instance of BaseHandler")

        if priority not in self._handlers:
            self._handlers[priority] = []

        self._handlers[priority].append(handler)

    def register_fallback(self, handler: BaseHandler) -> None:
        """Register a fallback handler for unknown elements.

        Args:
            handler: Fallback handler instance

        Raises:
            ValueError: If handler is not a BaseHandler instance
        """
        if not isinstance(handler, BaseHandler):
            raise ValueError("Fallback handler must be an instance of BaseHandler")

        self._fallback_handlers.append(handler)

    def get_handler(self, element: any) -> Optional[BaseHandler]:
        """Get the most appropriate handler for an element.

        Tries handlers in priority order, returns the first one
        that can handle the element with confidence > 0.

        Args:
            element: Element to find handler for

        Returns:
            BaseHandler if found, None if no handler can process
        """
        best_handler = None
        best_confidence = 0

        # Try regular handlers in priority order (lower number = higher priority)
        for priority in sorted(self._handlers.keys(), reverse=False):
            for handler in self._handlers[priority]:
                confidence = handler.can_handle(element)
                # If we find a perfect confidence handler (1.0), use it immediately
                if confidence == 1.0:
                    return handler
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_handler = handler

        # Only try fallback if we don't have a good match
        if best_confidence < 0.5:
            for handler in self._fallback_handlers:
                confidence = handler.can_handle(element)
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_handler = handler

        return best_handler

    def get_handlers_by_priority(self) -> List[BaseHandler]:
        """Get all handlers sorted by priority.

        Returns:
            List of handlers in priority order (highest first)
        """
        handlers = []
        # Sort in ascending order (lower number = higher priority)
        for priority in sorted(self._handlers.keys(), reverse=False):
            handlers.extend(self._handlers[priority])
        handlers.extend(self._fallback_handlers)
        return handlers

    def clear(self) -> None:
        """Remove all registered handlers."""
        self._handlers.clear()
        self._fallback_handlers.clear()
