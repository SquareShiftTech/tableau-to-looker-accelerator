"""End-to-end test for the complete migration pipeline."""

import logging
from pathlib import Path
import pytest

from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.models.xml_models import WorkbookModel

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)

SAMPLE_TWB = Path(
    "/mnt/c/squareshift/tableau-looker-vibe/vibe1/sample_twb_files/Book3.twb"
)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_pipeline_with_physical_joins():
    """Test pipeline with workbook containing physical joins."""
    # Initialize engine
    engine = MigrationEngine()

    # Process workbook
    result = engine.migrate_file(str(SAMPLE_TWB), str(SAMPLE_TWB.parent))

    # Validate result structure
    assert isinstance(result, WorkbookModel)
    assert result.metadata.source_file == str(SAMPLE_TWB)

    # Validate physical joins
    physical_joins = [r for r in result.elements.relationships if r.type == "physical"]
    assert len(physical_joins) == 2

    # Validate inner join
    inner_join = next(j for j in physical_joins if j.join_type == "inner")
    assert inner_join.tables[0].table == "[gke-elastic-394012.Movies].[movies_data]"
    assert inner_join.tables[1].table == "[gke-elastic-394012.Movies].[movies_data]"
    assert inner_join.expression.operator == "="
    assert "[movies_data].[id]" in inner_join.expression.expressions

    # Validate right join
    right_join = next(j for j in physical_joins if j.join_type == "right")
    assert right_join.tables[0].table == "[gke-elastic-394012.Movies].[movies_data]"
    assert right_join.tables[1].table == "[gke-elastic-394012.Movies].[movies_data]"
    assert right_join.expression.operator == "="
    assert "[movies_data2].[id]" in right_join.expression.expressions
