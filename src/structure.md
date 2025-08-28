# Structure Import 

## `format_importers.py`
 By separating format handling from the main process, you can easily add importer for JSON, XML, or any other format just by adding a new class to the importers file. The main controller and workflow never need to change.

- `PXImporter` - Reads PX files 
- `CSVImporter` - Reads CSV files  
- `IMPORTERS` registry - List of all available importers in the file

How it works:
1. Checks if a file is PX/CSV/etc
2. Downloads the file
3. Extracts column names and types
4. Returns standardized information

## `structure_importer.py` - main process
Manages the whole process
1. Gets your dataset list
2. For each dataset, finds files it can read
3. Uses the right importer from `format_importers.py`
4. Creates SHACL schema
5. Uploads to i14y API

Skips duplicate files (same PX file in different languages)


## The Workflow

1. Download dataset IDs from your harvester
2. Get API access token
3. Run `structure_importer.py`
4. Upload results log

## How to Add New Formats

Just add a new class to `format_importers.py`:

```python
class ExcelImporter:
    def can_process(self, distribution):
        return '.xlsx' in distribution.get('accessUrl', '')
    
    def download_and_parse(self, distribution):
        # Read Excel file and return structure info
        return {"identifier": "...", "properties": [...]}

# Add to registry
IMPORTERS["excel"] = ExcelImporter
```

The system automatically picks up new formats without changing anything else.

## What You Get

Clean SHACL schemas like this:
```turtle
i14y:myDataShape a sh:NodeShape ;
    rdfs:label "My Dataset"@en ;
    sh:property i14y:myDataShape/year,
                i14y:myDataShape/region .
```

## Files You Need

```
src/
├── format_importers.py    # Format handlers
├── structure_importer.py  # Main controller
└── .github/workflows/structure_import.yml  # Automation
```

That's it - the system handles PX files now, CSV files, and any format you add later.