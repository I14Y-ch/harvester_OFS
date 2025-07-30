# I14Y Harvester Documentation - OGD Statistical Federal Office (ABN)

The harvesting of the OGD Statistical Federal Office catalogue was done via Github actions in ABN. Synchronisation takes place automatically every day at midnight. Synchronisation can also be triggered manually if necessary. A log file is saved after each synchronisation.

## Process Overview

In detail, the process works as follows:

### 1. Authentication Process
- Obtains access token using client credentials
- Uses secrets `CLIENT_ID` and `CLIENT_SECRET` stored securely

### 2. Data Harvesting Process
a. **Loads previous state**: Reads stored dataset IDs from `dataset_ids.json`  
b. **Fetches current datasets**: Retrieves OGD from DAM API 
   (`https://dam-api.bfs.admin.ch/hub/api/ogd/harvest`)  
c. **Processes each dataset**:  
   i. Compares with previous version  
   ii. Identifies new, updated, unchanged or deleted datasets
   iii. Creates appropriate payload for I14Y API  

### 3. Dataset Processing Logic
a. **For new/updated datasets**:  
   i. Checks if the dataset is valid:  
      - At least one description for the dataset  
      - PDF distribution is discarded  
      - At least one distribution per dataset
   ii. Creates proper API payload  
   iii. Submits to I14Y API (`api-a.i14y.admin.ch`)  
   iv. For new datasets, automatically sets:  
      1. Status: "Recorded"  
      2. Level: "Public"  

### 4. Output and Logging
*(Logs retention: 2 days, configurable)*  

a. **Artifacts**:  
   - `dataset_ids.json`: Current state of all processed datasets  
   - `harvest_log.txt`: Detailed operation log  

b. **Log includes**:  
   - Timestamp of operation  
   - List of created datasets  
   - List of updated datasets  
   - List of unchanged datasets  

**Note**: All manipulations can be modified and adapted before the official harvesting on PROD.
