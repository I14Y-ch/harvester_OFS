# Structure Import 
## Required Files

```
src/
├── format_importers.py              # Format-specific importers
├── structure_importer.py            # Main controller  
└── .github/workflows/
    └── structure_import.yml          # workflow
```

## `format_importers.py`
Contains importers for different file types. By separating format handling from the main process, you can easily add support for Excel, JSON, XML, or any other format by adding a new class. The main controller and workflow never need to change.

- `PXImporter` - Reads PX files **TO ADJUST**
- `CSVImporter` - Reads CSV files **TO ADJUST**
- `IMPORTERS` registry - List of all available importers in the file

How it works:
1. Checks if a file is PX/CSV/etc
2. Downloads the file
3. Extracts column names and types
4. Returns standardized information

## `structure_importer.py` - Main Controller
managse the entire import process and reads harvest logs to determine which datasets need processing.

Process flow:
1. Reads harvest log to identify created/updated datasets
2. For each dataset that changed:
   - Fetches distributions from i14y API  
   - Finds compatible importer from `format_importers.py`
   - Deletes old structure (only for updates) 
   - Creates new SHACL schema
   - Uploads to i14y API
3. Skips unchanged or deleted datasets and handles duplicate files automatically

## The Workflow
Steps:
1. Download dataset IDs and harvest log from harvester workflow
2. Get API access token using secrets
3. Run `structure_importer.py` 
4. Upload execution log as artifact

## Adding New Formats
Add a new class to `format_importers.py` following this pattern:

```python
class ExcelImporter:
    def can_process(self, distribution):
        return '.xlsx' in distribution.get('accessUrl', '')
   
    def get_identifier(self, distribution):
        return distribution.get('accessUrl', '').split('/')[-1]
    
    def download_and_parse(self, distribution):
        # Download Excel file and analyze structure
        return {
            "identifier": "my_excel_file",
            "title": {"en": "Excel Data"},
            "description": {"en": "Structure from Excel file"},
            "properties": [
                {"name": "column1", "labels": {"en": "Column 1"}, "datatype": "string"}
            ]
        }

# Register the importer
IMPORTERS["excel"] = ExcelImporter
```

The main code uses new importers without code changes elsewhere.
