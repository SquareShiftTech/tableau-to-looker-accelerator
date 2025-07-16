from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class MigrationStatus(str, Enum):
    """Status of a migration operation"""

    SUCCESS = "success"
    PARTIAL = "partial"  # Some elements failed but overall succeeded
    FAILED = "failed"


class ElementError(BaseModel):
    """Details about an element that failed to migrate"""

    element_type: str
    element_id: Optional[str] = None
    error: str
    details: Optional[Dict] = None


class MigrationStats(BaseModel):
    """Statistics about the migration"""

    total_elements: int = 0
    successful_elements: int = 0
    failed_elements: int = 0
    processing_time: float = 0.0  # in seconds
    memory_used: Optional[float] = None  # in MB


class MigrationResult(BaseModel):
    """Complete results of a migration operation"""

    status: MigrationStatus
    source_file: str
    output_dir: str
    model_name: str
    stats: MigrationStats = Field(default_factory=MigrationStats)
    errors: List[ElementError] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    generated_files: List[str] = Field(default_factory=list)

    def add_error(
        self,
        element_type: str,
        error: str,
        element_id: Optional[str] = None,
        details: Optional[Dict] = None,
    ):
        """Add an error that occurred during migration"""
        self.errors.append(
            ElementError(
                element_type=element_type,
                element_id=element_id,
                error=error,
                details=details,
            )
        )
        self.stats.failed_elements += 1

    def add_warning(self, warning: str):
        """Add a warning message"""
        self.warnings.append(warning)

    def add_generated_file(self, file_path: str):
        """Add a file that was generated during migration"""
        self.generated_files.append(file_path)

    def update_status(self):
        """Update the overall status based on current stats"""
        if self.stats.failed_elements == 0:
            self.status = MigrationStatus.SUCCESS
        elif self.stats.failed_elements < self.stats.total_elements:
            self.status = MigrationStatus.PARTIAL
        else:
            self.status = MigrationStatus.FAILED
