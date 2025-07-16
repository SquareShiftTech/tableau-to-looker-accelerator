# Tableau to LookML Migration Engine - Setup Guide

## Overview

This project provides a clean, extensible architecture for migrating Tableau workbooks (.twb/.twbx) to LookML format. The engine uses a plugin-based handler system to process different element types with proper separation of concerns.

## Architecture

### Core Components

1. **XMLParser** - Parses Tableau workbook XML files
2. **MigrationEngine** - Orchestrates the conversion pipeline
3. **Handlers** - Process specific element types (dimensions, measures, connections, relationships)
4. **PluginRegistry** - Manages handler registration and priority

### Clean Architecture Principles

- **Single Responsibility**: Each component has one clear purpose
- **Separation of Concerns**: XML parsing, business logic, and orchestration are separate
- **Extensibility**: New handlers can be added without modifying core components

## Installation

### Prerequisites

- Python 3.8+
- [uv](https://docs.astral.sh/uv/) (modern Python package manager)

### Setup Steps

1. **Install uv** (if not already installed)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# or
pip install uv
```

2. **Setup Project**
```bash
cd /path/to/tableau-looker-vibe/vibe1/core

# Create virtual environment and install dependencies
uv sync

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install Package in Development Mode**
```bash
uv pip install -e .
```

## Usage

### Basic Usage

```python
from tableau_looker_lib.core.migration_engine import MigrationEngine

# Initialize engine
engine = MigrationEngine()

# Convert Tableau workbook
result = engine.migrate_file(
    tableau_file="path/to/workbook.twb",
    output_dir="path/to/output"
)

# Result contains structured JSON data
print(f"Found {len(result['dimensions'])} dimensions")
print(f"Found {len(result['measures'])} measures")
print(f"Found {len(result['relationships'])} relationships")
```

### Output Structure

The engine produces a unified JSON structure:

```json
{
  "metadata": {
    "source_file": "path/to/workbook.twb",
    "output_dir": "path/to/output"
  },
  "tables": [...],
  "relationships": [...],
  "connections": [...],
  "dimensions": [...],
  "measures": [...],
  "parameters": [...]
}
```

### Relationship Structure

Both physical and logical relationships use a unified structure:

```json
{
  "relationship_type": "physical|logical",
  "join_type": "inner|left|right|full",
  "expression": {
    "operator": "=",
    "expressions": ["[table1].[field1]", "[table2].[field2]"]
  },
  "tables": ["table1", "table2"],
  "table_aliases": {
    "alias1": "actual_table_name1",
    "alias2": "actual_table_name2"
  }
}
```

## Testing

### Run All Tests

```bash
# Activate virtual environment
source .venv/bin/activate

# Run all tests with coverage
uv run pytest tests/ -v --cov=src --cov-report=html

# Run specific test files
uv run pytest tests/test_book2_pipeline.py -v  # No relationships
uv run pytest tests/test_book3_pipeline.py -v  # Logical relationships
uv run pytest tests/test_book4_pipeline.py -v  # Physical joins
```

### Test Files

- **Book2.twb** - Simple workbook with no relationships
- **Book3.twb** - Contains logical relationships between tables
- **Book4.twb** - Contains physical joins with table aliases

## Handler Development

### Creating a New Handler

1. **Inherit from BaseHandler**
```python
from tableau_looker_lib.handlers.base_handler import BaseHandler

class MyCustomHandler(BaseHandler):
    def can_handle(self, data: Dict) -> float:
        # Return confidence score 0.0-1.0
        if data.get("type") == "my_element":
            return 1.0
        return 0.0

    def convert_to_json(self, raw_data: Dict) -> Dict:
        # Convert raw data to schema-compliant JSON
        return {
            "name": raw_data.get("name"),
            "type": "my_element"
        }
```

2. **Register Handler**
```python
engine = MigrationEngine()
engine.register_handler(MyCustomHandler(), priority=10)
```

### Handler Priority System

Handlers are processed by priority (lower = higher priority):
- RelationshipHandler (priority 1)
- ConnectionHandler (priority 2)
- DimensionHandler (priority 3)
- MeasureHandler (priority 4)
- ParameterHandler (priority 5)

## Supported Features

### ✅ Fully Supported

- **Physical Joins**: Inner, left, right, full joins with table aliases
- **Logical Relationships**: Object-graph relationships with endpoints
- **Dimensions**: All field types, semantic roles, calculations
- **Measures**: All aggregation types, formatting, drill-down
- **Connections**: BigQuery, federated, standard databases
- **Parameters**: Range and list parameters

### ⚠️ Partially Supported

- **Custom SQL**: Basic extraction (may need enhancement)
- **Complex Expressions**: Currently supports equality (extensible)

### 🔄 Future Enhancements

- **Fallback Handler**: For unknown elements
- **Union Operations**: Different structure than joins
- **Advanced Calculations**: Complex field calculations

## File Structure

```
core/
├── src/tableau_looker_lib/
│   ├── core/
│   │   ├── migration_engine.py    # Main orchestration
│   │   ├── xml_parser.py          # XML parsing logic
│   │   └── plugin_registry.py     # Handler management
│   ├── handlers/
│   │   ├── base_handler.py        # Handler interface
│   │   ├── dimension_handler.py   # Dimension processing
│   │   ├── measure_handler.py     # Measure processing
│   │   ├── connection_handler.py  # Connection processing
│   │   └── relationship_handler.py # Relationship processing
│   └── models/
│       ├── json_schema.py         # Output schema definitions
│       └── xml_models.py          # XML structure models
├── tests/
│   ├── test_book2_pipeline.py     # No relationships test
│   ├── test_book3_pipeline.py     # Logical relationships test
│   ├── test_book4_pipeline.py     # Physical joins test
│   └── unit/                      # Unit tests
└── sample_twb_files/              # Test workbooks
```

## Development Commands

### Using uv

```bash
# Install dependencies
uv sync

# Add new dependency
uv add package-name

# Add development dependency
uv add --dev package-name

# Run Python with uv
uv run python script.py

# Run tests
uv run pytest

# Update dependencies
uv lock --upgrade
```

## Troubleshooting

### Common Issues

1. **Empty Results**: Check that handlers are properly registered
2. **Missing Relationships**: Verify XML structure with debug output
3. **Table Alias Errors**: Ensure table aliases are properly mapped

### Debug Mode

Enable debug logging to see detailed processing:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

engine = MigrationEngine()
result = engine.migrate_file(tableau_file, output_dir)
```

### Output Files

The engine creates:
- `processed_pipeline_output.json` - Complete structured output
- Debug logs showing element processing and handler selection

## Contributing

1. Follow the clean architecture principles
2. Add tests for new features
3. Update this documentation
4. Ensure backward compatibility

## License

[Add your license information here]
