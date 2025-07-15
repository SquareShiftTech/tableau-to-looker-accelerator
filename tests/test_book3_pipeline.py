"""End-to-end test for the complete migration pipeline with Book3.twb."""

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

SAMPLE_TWB = Path(
    "sample_twb_files/Book3.twb"
)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_book3_pipeline_with_logical_joins():
    """Test pipeline with Book3.twb containing logical joins."""
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

    # Validate relationships (could be logical or physical)
    assert len(result["relationships"]) > 0

    # Validate join structure
    join = result["relationships"][0]
    assert join["join_type"] == "inner"
    assert join["expression"]["operator"] == "="
    assert len(join["expression"]["expressions"]) == 2

    # Validate tables
    assert len(result["tables"]) >= 2  # Should have credits and movies_data tables

    print("✅ Book3.twb test passed!")
    print(
        f"Found: {len(result['dimensions'])} dimensions, {len(result['measures'])} measures"
    )
    print(
        f"Found: {len(result['connections'])} connections, {len(result['relationships'])} relationships"
    )
