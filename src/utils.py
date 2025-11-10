import os
from time import time
import requests
import urllib
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def timer(func):
    # This function shows the execution time of
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f"Function {func.__name__!r} executed in {(t2-t1):.4f}s")
        return result

    return wrap_func


def get_access_token(client_key, client_key_secret):
    data = {"grant_type": "client_credentials"}
    response = requests.post(
        #TODO Sergiy: put in variable
        #"https://identity-eiam-r.eiam.admin.ch/realms/edi_bfs-i14y", # DEV
        # "https://identity.bit.admin.ch/realms/bfs-sis-p/protocol/openid-connect/token", # PROD
        "https://identity-a.bit.admin.ch/realms/bfs-sis-a/protocol/openid-connect/token", # ABN
        data=data,
        verify=False,
        auth=(client_key, client_key_secret),
    )
    if response.status_code >= 400:
        raise Exception("Failed to get token")
    return response.json()

# Debug function, checks in ABN which dataset have multiple csv files
def search_dataset_multiple_files():
    pass


def reauth_if_token_expired(func):
    # This function reruns the passed function with a new token if there is an 401 error
    def wrap_func(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except requests.HTTPError as e:
            print(f"API error: {e.response.status_code} - {e.response.text}")
            if e.response.status_code == 401:
                # TODO Sergiy: avoid to edit global variables from config.py, if you move this in a class it will be more clean to change the api_token afterwards
                API_TOKEN = get_access_token(os.environ['CLIENT_KEY'], os.environ['CLIENT_SECRET'])
                result = func(*args, **kwargs)

        return result

    return wrap_func

# TODO Sergiy: remove function before commit
@timer
def test_csv(link, first_bytes=True):
    if first_bytes:
        # Reads first MB of csv file
        csv_content = urllib.request.urlopen(link).read(1024**2)
        for encoding in ['utf-8', 'utf-8-sig', 'iso-8859-1']:
            try:
                content = csv_content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        print(content[:content.index("\n")])
    else:
        response = requests.get(link, timeout=30)
        response.raise_for_status()
        csv_content = response.content
        for encoding in ['utf-8', 'utf-8-sig', 'iso-8859-1']:
            try:
                content = csv_content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        print(content[:content.index("\n")])

# TODO Sergiy: remove function before commit
# check if getting all datasets is not too slow on ABN
@reauth_if_token_expired
@timer
def test_ABN_get_all_existing_datasets_identifier_id_map(publisherIdentifier: str, pageSize: int = 20) -> str:
    """Gets all existing datasets in one request"""
    all_existing_datasets_identifier_id_map = {}

    url = f"https://api-a.i14y.admin.ch/api/partner/v1/datasets"
    headers = {"Authorization": os.environ['ABN_ACCESS_TOKEN'], "Accept": "application/json"}
    i = 1
    has_more = True


    while has_more:
        params = {
            "publisherIdentifier": publisherIdentifier,
            "pageSize": pageSize,
            "page": i,
        }
        
        response = requests.get(url, params=params, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json()
        for x in data["data"]:
            dataset_id = x["id"]
            dataset_identifiers = x["identifiers"]
            for identifier in dataset_identifiers:
                all_existing_datasets_identifier_id_map[identifier] = dataset_id
        i += 1
        has_more = len(data["data"]) > 0


    return all_existing_datasets_identifier_id_map

# TODO Sergiy: remove function before commit
if __name__=="__main__":
    # csv_link="https://dam-api.bfs.admin.ch/hub/api/dam/assets/36133638/master"
    # test_csv(csv_link,first_bytes=False)
    # test_csv(csv_link,first_bytes=True)
    # a=test_ABN_get_all_existing_datasets_identifier_id_map("CH1",pageSize=100)
    # print(f"Number of dataset ids: {len(a)}")
    pass
