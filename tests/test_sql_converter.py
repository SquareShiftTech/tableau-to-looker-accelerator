import sys
import json
import pytest
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tableau_to_looker_parser.generators.lookml_sql_converter import LookMLSQLConverter

# ----------------------------
# 1. Check for unavailable dialect
# ----------------------------


def test_invalid_source_dialect():
    """Ensure using an unavailable source dialect raises ValueError."""

    with pytest.raises(ValueError) as exc:
        LookMLSQLConverter("not_a_real_db", "postgres")
    assert "Unsupported source dialect" in str(exc.value)


def test_invalid_target_dialect():
    """Ensure using an unavailable target dialect raises ValueError."""
    with pytest.raises(ValueError) as exc:
        LookMLSQLConverter("bigquery", "not_a_real_db")
    assert "Unsupported target dialect" in str(exc.value)


def test_invalid_source_and_target_dialect():
    """Ensure using both invalid dialects raises ValueError."""
    with pytest.raises(ValueError) as exc:
        LookMLSQLConverter("fake_source", "fake_target")
    assert "Unsupported source dialect" in str(
        exc.value
    ) or "Unsupported target dialect" in str(exc.value)


def test_case_insensitive_dialect_support():
    """Dialect names should be accepted regardless of case."""
    try:
        conv = LookMLSQLConverter("BigQuery", "Postgres")
        assert conv.source_dialect == "bigquery"
        assert conv.target_dialect == "postgres"
    except ValueError:
        pytest.fail("ValueError raised unexpectedly for valid dialects")


def test_valid_dialect_does_not_raise():
    """Ensure supported dialects are accepted."""
    try:
        conv = LookMLSQLConverter("bigquery", "postgres")
        assert conv.source_dialect == "bigquery"
        assert conv.target_dialect == "postgres"
    except ValueError:
        pytest.fail("ValueError raised unexpectedly for valid dialects")


# ----------------------------
# 2. Preprocessing Tests
# ----------------------------


@pytest.fixture
def converter():
    """Create a converter instance for testing."""
    # Use lowercase dialects to avoid case sensitivity issues
    return LookMLSQLConverter("bigquery", "postgres", verbose=False)


@pytest.mark.parametrize(
    "test_case",
    [
        pytest.param(case, id=case["name"])
        for case in json.load(
            open(Path(__file__).parent / "dataset/postgresql_test_data.json")
        )["test_cases"]
    ],
)
def test_preprocess_convertor(converter, test_case):
    input_sql = test_case["input_sql"]
    excepted_output_sql = test_case["output_sql"]

    output_sql = converter.convert(input_sql)

    # Check if the placeholder mapping matches expected
    assert output_sql == excepted_output_sql, (
        f"SQL mismatch for test '{test_case['name']}':\n"
        f"Input SQL: {input_sql}\n"
        f"Expected SQL: {excepted_output_sql}\n"
        f"Actual SQL: {output_sql}\n"
    )


# ----------------------------
# 3. Postgresql Convertion
# ----------------------------


@pytest.fixture
def postgresql_converter():
    """Create a converter instance for testing."""
    # Use lowercase dialects to avoid case sensitivity issues
    return LookMLSQLConverter("bigquery", "postgres", verbose=False)


@pytest.mark.parametrize(
    "test_case",
    [
        pytest.param(case, id=case["name"])
        for case in json.load(
            open(Path(__file__).parent / "dataset/postgresql_test_data.json")
        )["test_cases"]
    ],
)
def test_postgres_converter(postgresql_converter, test_case):
    input_sql = test_case["input_sql"]
    excepted_output_sql = test_case["output_sql"]

    output_sql = postgresql_converter.convert(input_sql)

    # Check if the placeholder mapping matches expected
    assert output_sql == excepted_output_sql, (
        f"SQL mismatch for test '{test_case['name']}':\n"
        f"Input SQL: {input_sql}\n"
        f"Expected SQL: {excepted_output_sql}\n"
        f"Actual SQL: {output_sql}\n"
    )
