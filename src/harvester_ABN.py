import requests
from config import *
from dcat_properties_utils import *
from rdflib import Graph, URIRef
from rdflib.namespace import DCTERMS, FOAF, RDFS, DCAT, RDF, RDFS
from rdflib import Namespace
import json
import os
from dateutil import parser
from typing import Dict, Any, List
from pathlib import Path
import datetime

# Namespaces
ADMS = Namespace("http://www.w3.org/ns/adms#")
SPDX = Namespace("http://spdx.org/rdf/terms#")
dcat3 = Namespace("http://www.w3.org/ns/dcat#")
SCHEMA = Namespace("http://schema.org/")


def change_level_i14y(id, level, token):
    """Change publication level of a dataset in i14y"""
    response = requests.put(
        url=f"{API_BASE_URL}/{id}/publication-level",
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
    return response.json()

def change_status_i14y(id, status, token):
    """Change registration status of a dataset in i14y"""
    response = requests.put(
        url=f"{API_BASE_URL}/{id}/registration-status",
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
    return response.json()

def is_valid_distribution(distribution):
    """Check if a distribution is valid (not PDF)."""
    if not distribution.get('mediaType'):
        return False
    
    # Check media type
    media_code = distribution['mediaType'].get('code', '').lower()
    excluded_media_types = ['application/pdf']
    
    # Check format if available
    format_code = None
    if distribution.get('format') and distribution['format'].get('code'):
        format_code = distribution['format']['code'].upper()
    
    # Exclusion list for format codes
    excluded_format_codes = ['PDF']
    
    # Distribution is invalid if:
    # 1. Media type is in excluded list OR
    # 2. Format code is in excluded list
    if (media_code in excluded_media_types) or (format_code in excluded_format_codes):
        return False
    
    return True

def has_valid_distributions(distributions):
    """Check if a dataset has at least one valid distribution."""
    if not distributions:
        return False
    return any(is_valid_distribution(dist) for dist in distributions)

def extract_dataset(graph, dataset_uri):
    """Extracts dataset details from RDF graph."""
    
    distributions = extract_distributions(graph, dataset_uri)
    
    if not has_valid_distributions(distributions):
        print(f"Skipping dataset {dataset_uri} - no valid distributions")
        return None
  
    original_identifier = get_literal(graph, dataset_uri, DCTERMS.identifier)
    dataset_number = None
    if original_identifier:
        # extract number from URL pattern like https://data.bl.ch/explore/dataset/10650/
        if "/dataset/" in original_identifier:
            dataset_number = original_identifier.split("/dataset/")[-1].rstrip("/")
    new_identifier = f"CH_KT_BL_dataset_{dataset_number}" if dataset_number else original_identifier

    dataset = { 
        "identifiers": [new_identifier, original_identifier] if dataset_number else [original_identifier],
        "title": get_multilingual_literal(graph, dataset_uri, DCTERMS.title),
        "description": get_multilingual_literal(graph, dataset_uri, DCTERMS.description),
        "accessRights": {"code": "PUBLIC"},  
        "issued": get_literal(graph, dataset_uri, DCTERMS.issued, is_date=True),
        "modified": get_literal(graph, dataset_uri, DCTERMS.modified, is_date=True),
        "publisher": DEFAULT_PUBLISHER, 
        "landingPages": get_resource_list(graph, dataset_uri, DCAT.landingPage),
        "keywords": get_multilingual_keywords(graph, dataset_uri, DCAT.keyword),
        "distributions": [dist for dist in distributions if is_valid_distribution(dist)],
        "languages": get_languages(graph, dataset_uri, DCTERMS.language),
        "contactPoints": extract_contact_points(graph, dataset_uri),
        "documentation": get_resource_list(graph, dataset_uri, FOAF.page),
        "images": get_resource_list(graph, dataset_uri, SCHEMA.image),
        "temporalCoverage": get_temporal_coverage(graph, dataset_uri), 
        "frequency": get_frequency(graph, dataset_uri),
        "isReferencedBy": get_is_referenced_by(graph, dataset_uri),
        "relations": get_relations(graph, dataset_uri),
        "spatial": get_spatial(graph, dataset_uri),
        "version": get_literal(graph, dataset_uri, dcat3.version),
        "versionNotes": get_multilingual_literal(graph, dataset_uri, ADMS.versionNotes),
        "conformsTo": get_conforms_to(graph, dataset_uri),
        "themes": get_themes(graph, dataset_uri, DCAT.theme), 
        "qualifiedRelations": [{"hadRole":{"code":"original"}, "relation":{"uri":get_literal(graph, dataset_uri, DCTERMS.identifier)}}]
        
    }

    if not dataset["description"]:
        print("no description found")
        return None

    return dataset

def fetch_datasets_from_api() -> List[Dict]:
    """Fetches a single test dataset from API for testing purposes"""
    datasets = []
    try:
        # Request just 1 dataset
        params = {"skip": 0, "limit": 100}
        response = requests.get(
            "https://data.bl.ch/api/explore/v2.1/catalog/exports/dcat",
            params=params,
            proxies=PROXIES,
            verify=False,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code}")
            return datasets
            
        if not response.text.strip():
            print("Received empty response")
            return datasets

        graph = Graph()
        graph.parse(data=response.text, format='xml')

        # Process just the first dataset we find
        for dataset_uri in list(graph.subjects(RDF.type, DCAT.Dataset))[:30]:
            print(f"Processing test dataset URI: {dataset_uri}")
            dataset = extract_dataset(graph, dataset_uri)
            
            if dataset and isinstance(dataset, dict):
                datasets.append(dataset)
                print("Successfully processed 1 test dataset")
            else:
                print(f"Skipping invalid test dataset: {dataset_uri}")
                
    except Exception as e:
        print(f"Error during test request: {e}")
    
    return datasets

# def fetch_datasets_from_api() -> List[Dict]:
#     """Fetches datasets directly from API and processes them."""
#     datasets = []
#     skip = 0
#     limit = 100
    
#     while True:
#         try:
#             params = {"skip": skip, "limit": limit}
#             response = requests.get(
#                 "https://dam-api.bfs.admin.ch/hub/api/ogd/harvest",
#                 params=params,
#                 proxies=PROXIES,
#                 verify=False,
#                 timeout=30
#             )
            
#             if response.status_code != 200:
#                 print(f"Error: Received status code {response.status_code}")
#                 break
                
#             if not response.text.strip():
#                 print("Received empty response - stopping")
#                 break

#             graph = Graph()
#             graph.parse(data=response.text, format='xml')

#             for dataset_uri in graph.subjects(RDF.type, DCAT.Dataset):
#                 print(f"Processing dataset URI: {dataset_uri}")
#                 dataset = extract_dataset(graph, dataset_uri)
                
#                 if dataset and isinstance(dataset, dict):
#                     datasets.append(dataset)
#                 else:
#                     print(f"Skipping invalid dataset: {dataset_uri}")
#             skip += limit

#             if len(graph) < limit:
#                 break
                
#         except requests.exceptions.RequestException as e:
#             print(f"Request failed: {e}")
#             break
#         except Exception as e:
#             print(f"Error processing response: {e}")
#             break
    
#     print(f"Total valid datasets found: {len(datasets)}")
#     return datasets

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

def extract_distributions(graph, dataset_uri):
    """Extracts distributions for a dataset."""
    distributions = []
    for distribution_uri in graph.objects(dataset_uri, DCAT.distribution):
        title = get_multilingual_literal(graph, distribution_uri, DCTERMS.title)
        description = get_multilingual_literal(graph, distribution_uri, DCTERMS.description)
        if not title: 
            title = {'de': 'Datenexport'}
        if not description:  
            description = {'de': 'Export der Daten'}

        media_type_uri = get_single_resource(graph, distribution_uri, DCAT.mediaType)
    
        format_uri = get_single_resource(graph, distribution_uri, DCTERMS.format)
        format_code = None

        if format_uri:
            format_uri_str = str(format_uri)
            if format_uri_str in FORMAT_TYPE_MAPPING:
                format_code = FORMAT_TYPE_MAPPING[format_uri_str]
            else:
                format_code = format_uri_str.split("/")[-1].upper()

        download_url = get_single_resource(graph, distribution_uri, DCAT.downloadURL)
        access_url = get_single_resource(graph, distribution_uri, DCAT.accessURL)
        common_url = access_url if access_url else download_url
        download_title = get_multilingual_literal(graph, distribution_uri, RDFS.label)
        availability_uri = get_single_resource(graph, distribution_uri, URIRef("http://data.europa.eu/r5r/availability"))
        license_uri = get_single_resource(graph, distribution_uri, DCTERMS.license)
        license_code = license_uri.split("/")[-1] if license_uri else None
        checksum_algorithm = get_literal(graph, distribution_uri, SPDX.checksumAlgorithm)
        checksum_value = get_literal(graph, distribution_uri, SPDX.checksumValue)
        packaging_format = get_literal(graph, distribution_uri, DCAT.packageFormat)

        distribution = {
            "title": title, 
            "description": description,  
            "format": {"code": format_code} if format_code and format_code in VALID_FORMAT_CODES else None,  
            "downloadUrl": {
                "label": download_title,  
                "uri": download_url if download_url else common_url
            } if common_url else None,
           "mediaType": {"code": get_media_type(media_type_uri)} if media_type_uri and get_media_type(media_type_uri) else None,
            "accessUrl": {
                "label": download_title,  
                "uri": common_url 
            } if common_url else None,
            "license": {"code": license_code} if license_code else None,  
            "availability": {"code": get_availability_code(availability_uri)} if get_availability_code(availability_uri) else None,  
            "issued": get_literal(graph, distribution_uri, DCTERMS.issued, is_date=True),
            "modified": get_literal(graph, distribution_uri, DCTERMS.modified, is_date=True),
            "rights": get_literal(graph, distribution_uri, DCTERMS.rights),
            "accessServices": get_access_services(graph, distribution_uri),
            "byteSize": get_literal(graph, distribution_uri, DCAT.byteSize),
            "checksum": {
                "algorithm": {"code": checksum_algorithm} if checksum_algorithm else None,
                "checksumValue": checksum_value
            } if checksum_algorithm or checksum_value else None,
            "conformsTo": get_conforms_to(graph, distribution_uri),
            "coverage": get_coverage(graph, distribution_uri),
            "documentation": get_resource_list(graph, distribution_uri, FOAF.page),
            "identifier": get_literal(graph, distribution_uri, DCTERMS.identifier),
            "images": get_resource_list(graph, distribution_uri, SCHEMA.image),
            "languages": get_languages(graph, distribution_uri, DCTERMS.language),
            "packagingFormat": {"code": packaging_format} if packaging_format else None,
            "spatialResolution": get_literal(graph, distribution_uri, DCAT.spatialResolutionInMeters), 
            "temporalResolution": get_literal(graph, distribution_uri, DCAT.temporalResolution)
        }
        distributions.append(distribution)
    return distributions

def create_dataset_payload(dataset):
    """Creates the JSON payload for the dataset submission."""
    if not isinstance(dataset, dict):
        raise ValueError("Dataset must be a dictionary.")
    return {"data": dataset}

def submit_to_api(payload, identifier=None, previous_ids=None):
    """Submits the dataset payload to the API."""
    headers = {
        "Authorization": API_TOKEN,
        "Content-Type": "application/json"
    }
    
    # Determine if we're creating or updating
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
    
    return response.json(), action

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

def main():
    # Create output directory if it doesn't exist
    OUTPUT_DIR = "api_responses"
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Get workspace and previous saved catalogue data
    workspace = os.getcwd()
    path_to_data = os.path.join(workspace, 'data', 'dataset_ids.json')
    
    try: 
        with open(path_to_data, 'r') as f:
            previous_ids = json.load(f)
            print("Successfully loaded previous data")
    except FileNotFoundError:
        previous_ids = {}  # Start with empty dict if no previous data
        print("No previous data found, starting fresh")
    
    # Get yesterday's date in UTC+1
    utc_plus_1 = datetime.timezone(datetime.timedelta(hours=1))
    now_utc_plus_1 = datetime.datetime.now(utc_plus_1)
    yesterday = now_utc_plus_1 - datetime.timedelta(days=1)

    created_datasets = []
    updated_datasets = []
    unchanged_datasets = []
    
    # Fetch datasets directly from API
    print("Fetching datasets from API...")
    datasets = fetch_datasets_from_api()
    
    print("\nStarting dataset import...\n")
    
    for dataset in datasets:
        identifier = dataset['identifiers'][0]
        print(f"\nProcessing dataset: {identifier}")
        print(f"Dataset: {identifier}")
        print(f"Issued date: {dataset.get('issued')}")
        print(f"Modified date: {dataset.get('modified')}")
        print(f"Yesterday cutoff: {yesterday}")

        modified_date = parse_date(dataset.get('modified'))
        created_date = parse_date(dataset.get('issued', dataset.get('modified')))
        
        try:
            # Check if this is a new dataset (not in previous_ids)
            is_new_dataset = identifier not in previous_ids
            
            if is_new_dataset or (created_date and created_date > yesterday):
                action = "created" if is_new_dataset else "updated"
                print(f"{action.capitalize()} dataset detected: {identifier}")
                payload = create_dataset_payload(dataset)
                response, action = submit_to_api(payload, identifier, previous_ids)
                
                if action == "created":
                    created_datasets.append(identifier)
                    previous_ids[identifier] = {'id': response.get('id', '')}
                    
                    try:
                        change_level_i14y(response['id'], 'Public', API_TOKEN)
                        change_status_i14y(response['id'], 'Registered', API_TOKEN)
                        print(f"Set i14y level to Public and status to Registered for {identifier}")
                    except Exception as e:
                        print(f"Error setting i14y level/status for {identifier}: {str(e)}")
                    
                elif action == "updated":
                    updated_datasets.append(identifier)
                    
                print(f"Success - Dataset {action}: {response}\n")
                
            else:
                unchanged_datasets.append(identifier)
                print(f"No changes detected for dataset: {identifier}\n")
                
        except Exception as e:
            print(f"Error processing dataset {identifier}: {str(e)}\n")
    
    # Save the updated IDs
    save_data(previous_ids, path_to_data)
    
    # Create log
    log = f"Harvest completed successfully at {datetime.datetime.now()}\n"
    log += f"Total datasets processed: {len(datasets)}\n"
    log += f"Created datasets ({len(created_datasets)}):\n"
    for item in created_datasets:
        log += f"- {item}\n"
    log += f"Updated datasets ({len(updated_datasets)}):\n"
    for item in updated_datasets:
        log += f"- {item}\n"
    log += f"Unchanged datasets ({len(unchanged_datasets)})\n"
    
    log_path = os.path.join(workspace, 'harvest_log.txt')
    with open(log_path, 'w') as f:
        f.write(log)
    
    print("\n=== Import Summary ===")
    print(f"Total processed: {len(datasets)}")
    print(f"Created: {len(created_datasets)}")
    print(f"Updated: {len(updated_datasets)}")
    print(f"Unchanged: {len(unchanged_datasets)}")
    print(f"Log saved to: {log_path}")

if __name__ == "__main__":
    main()


 
