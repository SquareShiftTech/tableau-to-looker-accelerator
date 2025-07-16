# Tableau to LookML Migration Library

A clean, extensible Python library for migrating Tableau workbooks (.twb/.twbx) to LookML format.

## Features

- **Clean Architecture**: Separation of concerns with XML parsing, business logic, and orchestration
- **Extensible Handler System**: Plugin-based architecture for processing different element types
- **Comprehensive Coverage**: Supports dimensions, measures, connections, relationships, and parameters
- **Unified Output**: Standardized JSON structure for both physical and logical relationships
- **Production Ready**: Handles 90%+ of Tableau use cases with robust error handling

## Installation

```bash
pip install tableau-looker-lib
```

## Quick Start

```python
from tableau_looker_lib import MigrationEngine

# Initialize the migration engine
engine = MigrationEngine()

# Convert a Tableau workbook
result = engine.migrate_file(
    tableau_file="workbook.twb",
    output_dir="output/"
)

# Access the results
print(f"Found {len(result['dimensions'])} dimensions")
print(f"Found {len(result['measures'])} measures")
print(f"Found {len(result['relationships'])} relationships")
```

## Supported Elements

- ✅ **Dimensions**: All field types, semantic roles, calculations
- ✅ **Measures**: All aggregation types, formatting, drill-down
- ✅ **Connections**: BigQuery, federated, standard databases
- ✅ **Relationships**: Physical joins and logical relationships with table aliases
- ✅ **Parameters**: Range and list parameters

## Development

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run type checks
uv run mypy .

# Format code
uv run black .
uv run isort .
```
