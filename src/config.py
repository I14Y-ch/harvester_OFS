import os

# OGD OFS API
API_OFS_URL = "https://dam-api.bfs.admin.ch/hub/api/ogd/harvest"

# I14Y API configuration
API_BASE_URL = "https://api.i14y.admin.ch/api/partner/v1"
API_BASE_URL_ABN = "https://api-a.i14y.admin.ch/api/partner/v1"
API_TOKEN = f"{os.environ['ACCESS_TOKEN']}" 

#IDS_I14Y = json.loads(os.environ['IDS_I14Y'])

# Organization settings
ORGANIZATION_ID =  "CH1"
DEFAULT_PUBLISHER = {
    "identifier": ORGANIZATION_ID
}

# File format (.xml and .rdf -> "xml", .ttl -> "ttl")
FILE_FORMAT = "xml"

# Proxies if necessary 
PROXIES = {
    "http": "http://proxy...",
    "https": "http://proxy..."
}
