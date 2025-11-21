# I14Y Harvester - OGD Statistical Federal Office (ABN + PROD)

The harvesting of the OGD Statistical Federal Office catalogue is done via Github actions in ABN and PROD. Synchronisation takes place automatically every day at midnight. Synchronisation can also be triggered manually if necessary. A log file is saved after each synchronisation.

## Features
- Import DCAT datasets from xml/rdf or ttl files to I14Y API
- Supported properties for dcat.Dataset:
  
| Property | Requirement level | 
| ----| ---- | 
| __dct:title__ | mandatory |
| __dct:description__ | mandatory | 
| __dct:accessRight__ (chosen from:  [PUBLIC, NON_PUBLIC, CONFIDENTIAL, RESTRICTED](http://publications.europa.eu/resource/authority/access-right)) | mandatory | 
| __dct:publisher__ (stated in config.py) | mandatory |
| __dct:identifier__ | mandatory |
| __dct:issued__ | optional |
| __dct:modified__ | optional |
| __dcat:landingPage__ | optional |
| __dcat:keyword__ | optional |
| __dct:language__ | optional |
| __dcat:contactPoint__ | optional |
| __documentation (foaf:page)__ | optional |
| __schema:image__ | optional |
| __dct:temporal__ | optional |
| __dcat:temporalResolution__ | optional |
| __frequency (dct:accrualPeriodicity)__ | optional |
| __dct:isReferencedBy__ | optional |
| __dct:relation__ | optional |
| __spatial/geographical coverage (dct:spatial)__ | optional |
| __dct:conformsTo__ | optional |
| __dcat:theme__ | optional |
| __dcat:version__ | optional |
| __adms:versionNotes__ | optional |


prov.qualifiedAttribution and prov.qualifiedRelation are not supported automatically, you can add those informations manually on I14Y.

- Supported properties for dcat.Distribution:
  
| Property | Requirement level | 
| ----| ---- | 
| __dct:title__ (if not stated, set automatically to 'Datenexport') | mandatory |
| __dct:description__ (if not stated, set automatically to 'Export der Daten') | mandatory |
| __dcat:accessURL__ | mandatory |
| __dcat:downloadURL__ | optional |
| __dct:license__ |optional |
| __dct:issued__ | optional |
| __dct:modified__ | optional |
| __dct:rights__ | optional |
| __dct:language__ | optional |
| __schema:image__ | optional |
| __dcat:spatialResolutionInMeters__ | optional |
| __dcat:temporalResolution__ | optional | |
| __dct:conformsTo__ | optional |
| __dcat:mediaType__ | optional |
| __dct:format__ | optional |
| __dct:packageFormat__ | optional |
| __spdx:checksum__ | optional |
| __dcat:byteSize__ | optional |
## Process Overview

In detail, the process works as follows:

### 1. Authentication Process
- Obtains access token using client credentials
- Uses secrets `CLIENT_ID` and `CLIENT_SECRET` stored securely

### 2. Data Harvesting Process
- **Fetches current datasets**: Retrieves OGD from DAM API (`https://dam-api.bfs.admin.ch/hub/api/ogd/harvest`)
- **Fetches already existing datasets from I14Y**: Gets all datasets for organisation CH1 with identifier xxx@bundesamt-fur-statistik-bfs
- **Processes each dataset**:
   - Compares with previous version
   - Identifies new, updated, unchanged or deleted datasets
   - Creates appropriate payload for I14Y API

### 3. Dataset Processing Logic
- **For new/updated datasets**:
   - Checks if the dataset is valid:
      - At least one description for the dataset
      - PDF distribution is discarded
      - At least one distribution per dataset
   - Creates proper API payload
   - Submits to I14Y API (`api-a.i14y.admin.ch` for ABN, `api-a.i14y.admin.ch` for PROD)
   - For new datasets, automatically sets:
      - Status: "Recorded"
      - Level: "Public"

### 4. Structures Import
- Once the datasets are imported, we also import the Structures for the datasets that were created or updated by the Harvester
- `datasets.json` is used by StructureImporter to know which datasets were created or updated by the harvester
- For now in the workflows, we import the structures only for ABN, not prod

### 5. Output and Logging
*(Logs retention: 2 days, configurable)*  

- **Artifacts**:  
   - `datasets.json`: State of processed datasets by harvester  
   - `harvest_log.txt`: Detailed operation log  for import of datasets
   - `structure_import_log.txt`: Detailed operation log for import of structures

- **Log includes**:  
   - Timestamp of operation  
   - List of created datasets  
   - List of updated datasets  
   - List of unchanged datasets  
   - List of deleted datasets
