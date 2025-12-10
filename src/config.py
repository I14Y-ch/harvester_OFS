# OGD OFS API
API_OFS_URL = "https://dam-api.bfs.admin.ch/hub/api/ogd/harvest"

# I14Y API configuration
API_BASE_URL_DEV = "https://iop-partner-d.app.cfap02.atlantica.admin.ch/api"
API_BASE_URL = "https://api.i14y.admin.ch/api/partner/v1"
API_BASE_URL_ABN = "https://api-a.i14y.admin.ch/api/partner/v1"

GET_TOKEN_URL_DEV = "https://identity-eiam-r.eiam.admin.ch/realms/edi_bfs-i14y"
GET_TOKEN_URL_ABN = "https://identity.i14y.a.c.bfs.admin.ch/realms/bfs-sis-a/protocol/openid-connect/token"
GET_TOKEN_URL_PROD = "https://identity.i14y.c.bfs.admin.ch/realms/bfs-sis-p/protocol/openid-connect/token"

# Organization settings
ORGANIZATION_ID =  "CH1"
DEFAULT_PUBLISHER = {
    "identifier": ORGANIZATION_ID
}

# File format (.xml and .rdf -> "xml", .ttl -> "ttl")
FILE_FORMAT = "xml"