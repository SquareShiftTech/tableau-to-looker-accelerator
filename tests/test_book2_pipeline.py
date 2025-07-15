"""End-to-end test for the complete migration pipeline with Book2.twb."""

import logging
from pathlib import Path
import pytest

from tableau_to_looker_parser.core.migration_engine import MigrationEngine

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)

SAMPLE_TWB = Path("sample_twb_files/Book2.twb")


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_book2_pipeline_no_relationships():
    """Test pipeline with Book2.twb containing no relationships."""
    # Initialize engine
    engine = MigrationEngine()

    # Process workbook
    result = engine.migrate_file(str(SAMPLE_TWB), str(SAMPLE_TWB.parent))

    # Validate result structure
    assert isinstance(result, dict)
    assert result["metadata"]["source_file"] == str(SAMPLE_TWB)

    # Validate we have data
    assert len(result["dimensions"]) > 0
    assert len(result["measures"]) > 0
    assert len(result["connections"]) > 0

    # Validate no relationships (Book2 should have no joins)
    assert len(result["relationships"]) == 0

    # Validate tables (should still have at least 1 table)
    assert len(result["tables"]) >= 1

    # Validate no parameters
    assert len(result["parameters"]) == 0

    print("✅ Book2.twb test passed!")
    print(
        f"Found: {len(result['dimensions'])} dimensions, {len(result['measures'])} measures"
    )
    print(
        f"Found: {len(result['connections'])} connections, {len(result['relationships'])} relationships"
    )
    print(
        f"Found: {len(result['tables'])} tables, {len(result['parameters'])} parameters"
    )
