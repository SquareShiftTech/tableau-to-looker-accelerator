from abc import ABC, abstractmethod
from typing import Dict


class BaseHandler(ABC):
    """Base class for all Tableau element handlers.

    Each handler is responsible for:
    1. Determining if it can handle raw data dict
    2. Converting raw data to standardized JSON format

    Handlers do NOT do XML parsing - that is XMLParser's responsibility.
    """

    @abstractmethod
    def can_handle(self, data: Dict) -> float:
        """Determine if this handler can process the raw data.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            float: Confidence score between 0.0 and 1.0
            0.0 = cannot handle
            1.0 = perfectly suited for this data
        """
        pass

    def process(self, data: Dict) -> Dict:
        """Process raw data completely.

        Template method that defines the processing algorithm.
        Subclasses should not override this method.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            Dict: Processed data in JSON format

        Raises:
            ProcessingError: If processing fails
        """
        # Validate we can handle this data
        confidence = self.can_handle(data)
        if confidence == 0.0:
            raise ValueError(
                f"Handler {self.__class__.__name__} cannot process this data"
            )

        # Convert to JSON format
        json_data = self.convert_to_json(data)

        # Add metadata
        json_data["_metadata"] = {
            "handler": self.__class__.__name__,
            "confidence": confidence,
        }

        return json_data

    @abstractmethod
    def convert_to_json(self, data: Dict) -> Dict:
        """Convert extracted data to intermediate JSON format.

        Args:
            data: Raw data from extract()

        Returns:
            Dict: Data conforming to JSON schema

        Raises:
            ConversionError: If data cannot be converted
        """
        pass

    def calculate_confidence(self, data: Dict) -> float:
        """Calculate confidence score for extracted data.

        Args:
            data: Raw extracted data

        Returns:
            float: Confidence score between 0.0 and 1.0
        """
        # Basic implementation - subclasses should override
        return 0.5 if data else 0.0

    def validate_input(self, data: Dict) -> bool:
        """Validate input data before processing.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            bool: True if valid, False otherwise
        """
        # Basic validation - subclasses should override
        return data is not None
