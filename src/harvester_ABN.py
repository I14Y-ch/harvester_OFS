import requests
from config import *
from dcat_properties_utils import *
from rdflib import Graph
from rdflib.namespace import DCAT, RDF
from structure import StructureImporter
import json
import os
from dateutil import parser
from typing import Dict, Any, List
import datetime
import time
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_datasets_from_api() -> List[Dict]:
    """Fetches first 3 datasets from API"""
    all_datasets = []
    skip = 0
    limit = 3  # Only fetch 3 datasets
    
    try:
        params = {"skip": skip, "limit": limit}
        response = requests.get(
            API_OFS_URL,
            params=params,
            verify=False,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            return all_datasets
            
        if not response.text.strip():
            print("Received empty response")
            return all_datasets

        graph = Graph()
        graph.parse(data=response.text, format='xml')
        
        dataset_uris = list(graph.subjects(RDF.type, DCAT.Dataset))[:3]  # Only take first 3
        
        for dataset_uri in dataset_uris:
            print(f"Processing dataset URI: {dataset_uri}")
            dataset = extract_dataset(graph, dataset_uri)
            
            if dataset and isinstance(dataset, dict):
                all_datasets.append(dataset)
            else:
                print(f"Skipping invalid dataset: {dataset_uri}")
        
        print(f"Processed {len(dataset_uris)} datasets in this batch")
        
    except Exception as e:
        print(f"Error during API request: {e}")
    
    print(f"Total datasets retrieved: {len(all_datasets)}")
    return all_datasets

def create_dataset_payload(dataset):
    """Creates the JSON payload for the dataset submission."""
    if not isinstance(dataset, dict):
        raise ValueError("Dataset must be a dictionary.")
    return {"data": dataset}

def change_level_i14y(id, level, token):
    """Change publication level of a dataset in i14y"""
    response = requests.put(
        url=f"{API_BASE_URL}/datasets/{id}/publication-level",
        params={'level': level}, 
        headers={
            'Authorization': token, 
            'Content-Type': 'application/json', 
            'Accept': '*/*',
            'Accept-encoding': 'json'
        }, 
        verify=False
    )
    response.raise_for_status()
    return response

def change_status_i14y(id, status, token):
    """Change registration status of a dataset in i14y"""
    response = requests.put(
        url=f"{API_BASE_URL}/datasets/{id}/registration-status",
        params={'status': status}, 
        headers={
            'Authorization': token, 
            'Content-Type': 'application/json', 
            'Accept': '*/*',
            'Accept-encoding': 'json'
        }, 
        verify=False
    )
    response.raise_for_status()
    return response

def submit_to_api(payload, identifier=None, previous_ids=None):
    """Submits the dataset payload to the API."""
    headers = {
        "Authorization": API_TOKEN,
        "Content-Type": "application/json"
    }

    action = "created"
    if identifier and previous_ids and identifier in previous_ids:
        dataset_id = previous_ids[identifier]['id']
        url = f"{API_BASE_URL}/datasets/{dataset_id}"
        response = requests.put(url, json=payload, headers=headers, verify=False)
        action = "updated"
    else:
        url = f"{API_BASE_URL}/datasets"
        response = requests.post(url, json=payload, headers=headers, verify=False)
    
    if response.status_code not in [200, 201, 204]:
        raise Exception(f"API error: {response.status_code} - {response.text}")

    return response.text, action

def parse_date(date_str):
    """Safely parse a date string, returning None if invalid or missing"""
    if not date_str:
        return None
    try:
        return parser.parse(date_str)
    except (ValueError, TypeError):
        return None

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
    
    # Initialize structure importer
    structure_importer = StructureImporter(API_TOKEN)
    
    # Get yesterday's date in UTC+1
    utc_plus_1 = datetime.timezone(datetime.timedelta(hours=1))
    now_utc_plus_1 = datetime.datetime.now(utc_plus_1)
    yesterday = now_utc_plus_1 - datetime.timedelta(days=1)

    created_datasets = []
    updated_datasets = []
    unchanged_datasets = []
    
    print("Fetching datasets from API...")
    datasets = fetch_datasets_from_api()  # Get all datasets
    
    print("\nStarting dataset import (FORCING UPDATE OF ALL DATASETS)...\n")
    
    current_source_identifiers = {dataset['identifiers'][0] for dataset in datasets}
    
    for dataset in datasets:
        identifier = dataset['identifiers'][0]
        print(f"\nProcessing dataset: {identifier}")
        print(f"Issued date: {dataset.get('issued')}")
        print(f"Modified date: {dataset.get('modified')}")

        try:
            # FORCE UPDATE ALL DATASETS REGARDLESS OF MODIFICATION DATE
            action = "updated" if identifier in previous_ids else "created"
            print(f"FORCED {action.capitalize()} dataset: {identifier}")

            payload = create_dataset_payload(dataset)
            response_id, action = submit_to_api(payload, identifier, previous_ids)
            response_id = response_id.strip('"')

            if action == "created":
                created_datasets.append(identifier)
                previous_ids[identifier] = {'id': response_id} 

                try:
                    change_level_i14y(response_id, 'Public', API_TOKEN)  
                    time.sleep(0.5)
                    change_status_i14y(response_id, 'Recorded', API_TOKEN)
                    print(f"Set i14y level to Public and status to Registered for {identifier}")
                except Exception as e:
                    print(f"Error setting i14y level/status for {identifier}: {str(e)}")
            elif action == "updated":
                updated_datasets.append(identifier)

            # Process PX structures after successful dataset creation/update
            if 'distributions' in dataset:
                    print("Checking for PX distributions...")
                    processed_structure = False
                    
                    for distribution in dataset['distributions']:
                        if distribution.get('format') == 'px' or \
                           any(ext in distribution.get('mediaType', '').lower() 
                              for ext in ['px', 'application/x-px']):
                            print(f"Found PX distribution: {distribution.get('title')}")
                            if structure_importer.process_px_distribution(distribution, response_id):
                                print(f"Successfully processed structure for {identifier}")
                                processed_structure = True
                                break  # Stop after first successful PX processing
                    
                    if not processed_structure:
                        print(f"No PX structure found or error processing structure for dataset {identifier}")

                print(f"Success - Dataset {action}: {response_id}\n")

        except Exception as e:
            print(f"Error processing dataset {identifier}: {str(e)}\n")

    # Save the dataset IDs
    os.makedirs(os.path.dirname(path_to_data), exist_ok=True)
    with open(path_to_data, 'w') as f:
        json.dump(previous_ids, f)

    # Create log
    log = f"FORCED UPDATE completed at {datetime.datetime.now()}\n"
    log += "Created datasets:\n"
    for item in created_datasets:
        log += f"\n- {item}"
    log += "\nUpdated datasets:\n"
    for item in updated_datasets:
        log += f"\n- {item}"
    log += "\nUnchanged datasets:\n"
    for item in unchanged_datasets:
        log += f"\n- {item}"

    # Save log in root directory
    log_path = os.path.join(workspace, 'harvest_log.txt')
    with open(log_path, 'w') as f:
        f.write(log)
    
    print("\n=== FORCED UPDATE Summary ===")
    print(f"Total processed: {len(datasets)}")
    print(f"Created: {len(created_datasets)}")
    print(f"Updated: {len(updated_datasets)}")
    print(f"Unchanged: {len(unchanged_datasets)}")

if __name__ == "__main__":
    main()
