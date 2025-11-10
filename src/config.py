import os

# OGD OFS API
API_OFS_URL = "https://dam-api.bfs.admin.ch/hub/api/ogd/harvest"

# I14Y API configuration
API_BASE_URL_DEV = "https://iop-partner-d.app.cfap02.atlantica.admin.ch/api"
API_BASE_URL = API_BASE_URL_DEV
API_BASE_URL_ABN = API_BASE_URL_DEV
# Sergiy: Temporary overwrite of API URL in order to not break anything TODO: don't forget to revert back
# API_BASE_URL = "https://api.i14y.admin.ch/api/partner/v1"
# API_BASE_URL_ABN = "https://api-a.i14y.admin.ch/api/partner/v1"

API_TOKEN = f"{os.environ['ACCESS_TOKEN']}"

#IDS_I14Y = json.loads(os.environ['IDS_I14Y'])

# Organization settings
# Sergiy: Temporary use test organization TODO: don't forget to revert back
# ORGANIZATION_ID =  "CH1"
ORGANIZATION_ID ="i14y-test-organisation"
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
