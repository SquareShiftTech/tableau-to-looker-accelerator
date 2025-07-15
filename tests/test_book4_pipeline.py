"""End-to-end test for the complete migration pipeline with Book4.twb."""

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
    "/mnt/c/squareshift/tableau-looker-vibe/vibe1/sample_twb_files/Book4.twb"
)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_book4_pipeline_with_physical_joins():
    """Test pipeline with Book4.twb containing physical joins."""
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

    # Validate physical relationships
    physical_joins = [
        r for r in result["relationships"] if r["relationship_type"] == "physical"
    ]
    assert len(physical_joins) > 0

    # Validate join structure
    join = physical_joins[0]
    assert join["join_type"] in ["inner", "left", "right", "full"]
    assert join["expression"]["operator"] == "="
    assert len(join["expression"]["expressions"]) == 2
    assert len(join["tables"]) >= 1

    # Validate tables
    assert len(result["tables"]) >= 1

    print("✅ Book4.twb test passed!")
    print(
        f"Found: {len(result['dimensions'])} dimensions, {len(result['measures'])} measures"
    )
    print(
        f"Found: {len(result['connections'])} connections, {len(result['relationships'])} relationships"
    )
    print(
        f"Found: {len(result['tables'])} tables, {len(result['parameters'])} parameters"
    )

    # Print relationship details
    for i, rel in enumerate(result["relationships"]):
        print(
            f"  Relationship {i + 1}: {rel['relationship_type']} {rel['join_type']} join"
        )
        print(f"    Expression: {rel['expression']['expressions']}")
        print(f"    Tables: {rel['tables']}")
