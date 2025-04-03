# I14Y API configuration
API_BASE_URL = "https://api-a.i14y.admin.ch/api/partner/v1"
API_TOKEN = "Bearer eyJraWQiOiI5YT"

# Organization settings
ORGANIZATION_ID = "CH1"
DEFAULT_PUBLISHER = {
    "identifier": ORGANIZATION_ID
}

# File paths
TEMPLATE_PATH = "api_responses/response_1.xml"

# File format (.xml and .rdf -> "xml", .ttl -> "ttl")
FILE_FORMAT = "xml"


# Configuration from config.py
API_BASE_URL = API_BASE_URL
API_TOKEN = API_TOKEN
ORGANIZATION_ID = ORGANIZATION_ID
DEFAULT_PUBLISHER = DEFAULT_PUBLISHER
TEMPLATE_PATH = TEMPLATE_PATH
FILE_FORMAT = FILE_FORMAT

# Proxy configuration (if needed)
PROXIES = {
    "http": os.environ.get('HTTP_PROXY', ''),
    "https": os.environ.get('HTTPS_PROXY', '')
}