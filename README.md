# Tableau to Looker Assessment Accelerator

## Prerequisites

- **Python 3.11** (required)

## Installation

1. Create a virtual environment:
   python -m venv t2l-env

2. Activate the virtual environment:
   # On Windows
   t2l-env\Scripts\activate
   
   # On Linux/Mac
   source t2l-env/bin/activate


3. Install the package from GitHub:
   
   pip install git+https://github.com/SquareShiftTech/tableau-to-looker-accelerator.git@v2.0.4
   

## Usage

### Extract from Tableau Server

To download workbooks from a Tableau server and generate JSON:

tableau-assess --server --server-url https://tableau.xxxx.xxx --username admin --password xxx --site-id xxx --generate-json


**Parameters:**
- `--server`: Enable server mode
- `--server-url`: Tableau server URL
- `--username`: Tableau username
- `--password`: Tableau password
- `--site-id`: Tableau site ID
- `--generate-json`: Generate JSON output (required)

### Process Local File

To process a local Tableau workbook file (.twb or .twbx) and generate JSON:

tableau-assess --local "full path" --generate-json


**Example:**
tableau-assess --local "C:\Users\User\Downloads\Hierarchy-and-Actions.twb" --generate-json


**Parameters:**
- `--local`: Path to local TWB/TWBX file
- `--generate-json`: Generate JSON output (required)

## Output

The generated JSON files will be saved in the `output` of your current directory by default.
