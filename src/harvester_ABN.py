import requests
from config import *
from dcat_properties_utils import *
from rdflib import Graph
from rdflib.namespace import DCAT, RDF
import json
import os
from dateutil import parser
from typing import Dict, Any, List
import datetime
import time
import urllib3
import traceback
from utils import reauth_if_token_expired, timer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_datasets_from_api() -> List[Dict]:
    """Fetches all datasets from API"""
    all_datasets = []
    skip = 0
    limit = 100 
    has_more = True
    
    while has_more:
        try:
            params = {"skip": skip, "limit": limit}
            response = requests.get(
                API_OFS_URL,
                params=params,
                #proxies=PROXIES,
                verify=False,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code}")
                break
                
            if not response.text.strip():
                print("Received empty response")
                break

            graph = Graph()
            graph.parse(data=response.text, format='xml')
            
            dataset_uris = list(graph.subjects(RDF.type, DCAT.Dataset))
            if not dataset_uris:
                has_more = False
                break
                
            for dataset_uri in dataset_uris:
                print(f"Processing dataset URI: {dataset_uri}")
                dataset = extract_dataset(graph, dataset_uri)
                
                if dataset and isinstance(dataset, dict):
                    all_datasets.append(dataset)
                else:
                    print(f"Skipping invalid dataset: {dataset_uri}")
            
            print(f"Processed {len(dataset_uris)} datasets in this batch")
            skip += limit
            # Sergiy: break for debug, in order to not iterate over all the datasets TODO: REMOVE
            if skip>=400:
                break
            
        except Exception as e:
            print(f"Error during API request: {e}")
            break
    
    print(f"Total datasets retrieved: {len(all_datasets)}")
    return all_datasets

def parse_rdf_file(file_path):
    """Parses an RDF file and extracts datasets with valid distributions."""
    graph = Graph()
    graph.parse(file_path, format=FILE_FORMAT)  
    
    datasets = []
    for dataset_uri in graph.subjects(RDF.type, DCAT.Dataset):
        print(f"Processing dataset URI: {dataset_uri}")  
        dataset = extract_dataset(graph, dataset_uri)

        if dataset and isinstance(dataset, dict):
            datasets.append(dataset)
        else:
            print(f"Skipping invalid dataset: {dataset_uri}") 
    print(f"Total valid datasets found: {len(datasets)}") 
    return datasets


def create_dataset_payload(dataset):
    """Creates the JSON payload for the dataset submission."""
    if not isinstance(dataset, dict):
        raise ValueError("Dataset must be a dictionary.")
    return {"data": dataset}


@timer
@reauth_if_token_expired
def change_level_i14y(id, level, token):
    """Change publication level of a dataset in i14y"""
    response = requests.put(
        url=f"{API_BASE_URL_ABN}/datasets/{id}/publication-level",
        params={'level': level}, 
        headers={
            'Authorization': token, 
            'Content-Type': 'application/json', 
            'Accept': '*/*',
            'Accept-encoding': 'json'
        }, 
        verify=False, 
        #proxies=PROXIES
    )
    response.raise_for_status()
    return response


@timer
@reauth_if_token_expired
def get_all_existing_datasets_identifier_id_map(publisherIdentifier: str, pageSize: int = 50) -> str:
    """Gets all existing datasets in one request"""
    all_existing_datasets_identifier_id_map = {}

    url = f"{API_BASE_URL_ABN}/datasets"
    headers = {"Authorization": API_TOKEN, "Accept": "application/json"}
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

@timer
@reauth_if_token_expired
def check_dataset_exists(identifier: str) -> str:
    """Checks if a dataset with the given identifier already exists."""
    url = f"{API_BASE_URL_ABN}/datasets"
    params = {"datasetIdentifier": identifier}
    headers = {
        "Authorization": API_TOKEN,
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, params=params, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json()

        if "data" in data and len(data["data"]) > 0:
            return data["data"][0]["id"]
        return None
    except Exception as e:
        print(f"Error checking dataset existence for identifier {identifier}: {e}")
        return None

@timer
@reauth_if_token_expired
def change_status_i14y(id, status, token):
    """Change registration status of a dataset in i14y"""
    response = requests.put(
        url=f"{API_BASE_URL_ABN}/datasets/{id}/registration-status",
        params={'status': status}, 
        headers={
            'Authorization': token, 
            'Content-Type': 'application/json', 
            'Accept': '*/*',
            'Accept-encoding': 'json'
        }, 
        verify=False, 
        #proxies=PROXIES
    )
    response.raise_for_status()
    return response

@timer
@reauth_if_token_expired
def submit_to_api(payload, identifier=None, previous_ids=None):
    """Submits the dataset payload to the API."""
    headers = {
        "Authorization": API_TOKEN,
        "Content-Type": "application/json"
    }

    action = "created"
    if identifier and previous_ids and identifier in previous_ids.keys():
        dataset_id = previous_ids[identifier]['id']
        url = f"{API_BASE_URL_ABN}/datasets/{dataset_id}"
        response = requests.put(url, json=payload, headers=headers, verify=False)
        action = "updated"
    else:
        payload["data"]["registrationStatus"] = "Recorded"
        payload["data"]["publicationLevel"] = "Public"
        url = f"{API_BASE_URL_ABN}/datasets"
        response = requests.post(url, json=payload, headers=headers, verify=False)

    if response.status_code not in {200, 201, 204}:
        response.raise_for_status()
        # raise requests.HTTPError(f"API error: {response.status_code} - {response.text}")
    

    return response.text, action


def save_data(data: Dict[str, Any], file_path: str) -> None:
    """Saves data to a JSON file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    try:
        with open(file_path, 'w') as file:
            json.dump(data, file)
    except IOError as e:
        print(f"Error saving data to {file_path}: {e}")

def parse_date(date_str):
    """Safely parse a date string, returning None if invalid or missing"""
    if not date_str:
        return None
    try:
        return parser.parse(date_str)
    except (ValueError, TypeError):
        return None

@timer
def main():
    workspace = os.getcwd()
    path_to_data = os.path.join(workspace, 'OGD_OFS', 'data', 'dataset_ids.json')

    try: 
        with open(path_to_data, 'r') as f:
            previous_ids = json.load(f)
            print("Successfully loaded previous data")
    except FileNotFoundError:
        previous_ids = {}  
        print("No previous data found, starting fresh")

    # Get yesterday's date in UTC+1
    utc_plus_1 = datetime.timezone(datetime.timedelta(hours=1))
    now_utc_plus_1 = datetime.datetime.now(utc_plus_1)
    yesterday = now_utc_plus_1 - datetime.timedelta(days=1)

    created_datasets = []
    updated_datasets = []
    unchanged_datasets = []
    deleted_datasets = []
    encountered_exception = False

    try:

        print("Fetching datasets from API...")
        datasets = fetch_datasets_from_api()

        print("\nStarting dataset import...\n")

        current_source_identifiers = {dataset['identifiers'][0] for dataset in datasets}

        all_existing_datasets_identifier_id_map = (
            get_all_existing_datasets_identifier_id_map(ORGANIZATION_ID)
        )

        for dataset in datasets:
            identifier = dataset['identifiers'][0]
            print(f"\nProcessing dataset: {identifier}")
            print(f"Issued date: {dataset.get('issued')}")
            print(f"Modified date: {dataset.get('modified')}")

            modified_date = parse_date(dataset.get('modified'))
            created_date = parse_date(dataset.get('issued', dataset.get('modified')))

            try:
                is_new_dataset = identifier not in previous_ids.keys()
                is_updated_dataset = modified_date and modified_date > yesterday

                # If for some reason the log is not available, then the datasets we try
                # to write but already exist will be noted as unchanged
                existing_dataset_id = all_existing_datasets_identifier_id_map[identifier] if identifier in all_existing_datasets_identifier_id_map.keys() else None
                if existing_dataset_id and not is_updated_dataset:
                    previous_ids[identifier] = {'id': existing_dataset_id} 
                    unchanged_datasets.append(identifier)
                    print(f"No changes detected for dataset: {identifier} (already exists)\n")

                elif is_new_dataset or is_updated_dataset:
                    action = "created" if is_new_dataset else "updated"
                    print(f"{action.capitalize()} dataset detected: {identifier}")

                    payload = create_dataset_payload(dataset)
                    response_id, action = submit_to_api(payload, identifier, previous_ids)
                    response_id = response_id.strip('"')

                    if action == "created":
                        created_datasets.append(identifier)
                        previous_ids[identifier] = {'id': response_id} 

                    elif action == "updated":
                        updated_datasets.append(identifier)

                    print(f"Success - Dataset {action}: {response_id}\n")

                else:
                    unchanged_datasets.append(identifier)
                    print(f"No changes detected for dataset: {identifier}\n")

            except Exception as e:
                print(f"Error processing dataset {identifier}: {str(e)}\n")
                print(traceback.format_exc())

        # Find datasets that exist in previous_ids but not in current source
        datasets_to_delete = set(previous_ids.keys()) - current_source_identifiers

        for identifier in datasets_to_delete:
            try:
                dataset_id = previous_ids[identifier]['id']
                headers = {
                    "Authorization": API_TOKEN,
                    "Content-Type": "application/json"
                }
                try:
                    change_level_i14y(dataset_id, 'Internal', API_TOKEN)
                    print(f"Changed publication level to Internal for {identifier}")
                except Exception as e:
                    print(f"Error changing publication level for {identifier}: {str(e)}")
                    continue  # Skip deletion if we can't change the level
                url = f"{API_BASE_URL_ABN}/datasets/{dataset_id}"
                response = requests.delete(url, headers=headers, verify=False)

                if response.status_code in [200, 204]:
                    deleted_datasets.append(identifier)
                    del previous_ids[identifier]
                    print(f"Successfully deleted dataset: {identifier}")
                else:
                    print(f"Failed to delete dataset {identifier}: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"Error deleting dataset {identifier}: {str(e)}")

    except Exception:
        encountered_exception = True
        print("Previously unhandled exception")

    finally:
        encountered_exception |= len(datasets) > len(created_datasets) + len(updated_datasets) + len(unchanged_datasets) + len(deleted_datasets)
        os.makedirs(os.path.dirname(path_to_data), exist_ok=True)

        with open(path_to_data, 'w') as f:
            json.dump(previous_ids, f)

        # Create log
        log = f"Harvest completed successfully at {datetime.datetime.now()}\n"
        if encountered_exception:
            log = f"Harvest encountered exception at {datetime.datetime.now()}\n"
        log += "Created datasets:\n"
        for item in created_datasets:
            log += f"\n- {item}"
        log += "\nUpdated datasets:\n"
        for item in updated_datasets:
            log += f"\n- {item}"
        log += "\nUnchanged datasets:\n"
        for item in unchanged_datasets:
            log += f"\n- {item}"
        log += "\nDeleted datasets:\n"
        for item in deleted_datasets:
            log += f"\n- {item}"

        if encountered_exception:
            log += "\nDatasets with exception:\n"
            for item in (
                set([x['identifiers'][0] for x in datasets])
                - set(created_datasets)
                - set(updated_datasets)
                - set(unchanged_datasets)
                - set(deleted_datasets)
            ):
                log += f"\n- {item}"

        # Save log in root directory
        log_path = os.path.join(workspace, 'harvest_log.txt')
        with open(log_path, 'w') as f:
            f.write(log)

        print("\n=== Import Summary ===")
        print(f"Total processed: {len(datasets)}")
        print(f"Created: {len(created_datasets)}")
        print(f"Updated: {len(updated_datasets)}")
        print(f"Unchanged: {len(unchanged_datasets)}")
        print(f"Deleted: {len(deleted_datasets)}")
        print(f"Log saved to: {log_path}")

if __name__ == "__main__":
    main()
