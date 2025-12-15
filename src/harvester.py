import requests
from common import CommonI14YAPI, reauth_if_token_expired
from config import *
from dcat_properties_utils import *
from rdflib import Graph
from rdflib.namespace import DCAT, RDF
import json
import os
from dateutil import parser
from typing import Dict, Any, List
import datetime
import urllib3
import traceback
from structure_importer import StructureImporter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class HarvesterOFS(CommonI14YAPI):

    def __init__(self, api_params):
        """
        api_params must be a dict containing:
        - client_key: client key to generate token
        - client_secret: client secret to generate token
        - api_get_token_url: url to generate token
        - api_base_url: url for i14y api calls
        - organization: i14y organization
        """
        super().__init__(api_params)

    def fetch_datasets_from_api(self) -> List[Dict]:
        """Fetches all datasets from API"""
        all_datasets = []
        skip = 0
        limit = 100
        has_more = True

        while has_more:
        
            params = {"skip": skip, "limit": limit}
            response = requests.get(
                API_OFS_URL,
                params=params,
                verify=False,
                timeout=30,
            )

            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code}")
                break

            if not response.text.strip():
                print("Received empty response")
                break

            graph = Graph()
            graph.parse(data=response.text, format="xml")

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

        print(f"Total datasets retrieved: {len(all_datasets)}")
        return all_datasets

    def parse_rdf_file(self, file_path):
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

    def create_dataset_payload(self, dataset):
        """Creates the JSON payload for the dataset submission."""
        if not isinstance(dataset, dict):
            raise ValueError("Dataset must be a dictionary.")
        return {"data": dataset}

    @reauth_if_token_expired
    def change_level_i14y(self, id, level):
        """Change publication level of a dataset in i14y"""
        response = requests.put(
            url=f"{self.api_base_url}/datasets/{id}/publication-level",
            params={"level": level},
            headers={
                "Authorization": self.api_token,
                "Content-Type": "application/json",
                "Accept": "*/*",
                "Accept-encoding": "json",
            },
            verify=False,
        )
        response.raise_for_status()
        return response

    @reauth_if_token_expired
    def change_status_i14y(self, id, status):
        """Change registration status of a dataset in i14y"""
        response = requests.put(
            url=f"{self.api_base_url}/datasets/{id}/registration-status",
            params={"status": status},
            headers={
                "Authorization": self.api_token,
                "Content-Type": "application/json",
                "Accept": "*/*",
                "Accept-encoding": "json",
            },
            verify=False,
        )
        response.raise_for_status()
        return response

    @reauth_if_token_expired
    def delete_i14y(self, dataset_id):
        headers = {"Authorization": self.api_token, "Content-Type": "application/json"}
        try:
            url_structures = f"{self.api_base_url}/datasets/{dataset_id}/structures"
            requests.delete(url_structures, headers=headers, verify=False)
        except requests.HTTPError as e:
            print(f"Error trying to delete structure: {e.response.status_code} - {e.response.text}")
        url = f"{self.api_base_url}/datasets/{dataset_id}"
        response = requests.delete(url, headers=headers, verify=False)
        response.raise_for_status()

        return response

    def get_all_identifier_id_map(self, datasets):
        all_existing_datasets_identifier_id_map = {}
        for dataset in datasets:
            dataset_id = dataset["id"]
            dataset_identifiers = dataset["identifiers"]
            for identifier in dataset_identifiers:
                all_existing_datasets_identifier_id_map[identifier] = dataset_id

        return all_existing_datasets_identifier_id_map

    @reauth_if_token_expired
    def submit_to_api(self, payload, identifier=None, previous_ids=None):
        """Submits the dataset payload to the API."""
        headers = {"Authorization": self.api_token, "Content-Type": "application/json"}

        action = "created"
        if identifier and previous_ids and identifier in previous_ids.keys():
            dataset_id = previous_ids[identifier]["id"]
            url = f"{self.api_base_url}/datasets/{dataset_id}"
            response = requests.put(url, json=payload, headers=headers, verify=False)
            action = "updated"
        else:
            url = f"{self.api_base_url}/datasets"
            response = requests.post(url, json=payload, headers=headers, verify=False)

        if response.status_code not in {200, 201, 204}:
            response.raise_for_status()

        return response.text, action

    def parse_date(self, date_str):
        """Safely parse a date string, returning None if invalid or missing"""
        if not date_str:
            return None
        try:
            return parser.parse(date_str)
        except (ValueError, TypeError):
            return None

    def harvest(self):

        # Get yesterday's date in UTC+1
        utc_plus_1 = datetime.timezone(datetime.timedelta(hours=1))
        now_utc_plus_1 = datetime.datetime.now(utc_plus_1)
        yesterday = now_utc_plus_1 - datetime.timedelta(days=1)

        """
        This dict has as key the status of the dataset (created/updated/unchanged/deleted)
            created/updated/unchanged/deleted contains a dict with the bfs identifier as key and i14y id as value
        dataset_status_identifier_id_map : {
            "created" : {
                "bfs_identifier": i14y_id
            },
            "updated" : {
                "bfs_identifier": i14y_id
            },
            "unchanged" : {
                "bfs_identifier": i14y_id
            },
            "deleted" : {
                "bfs_identifier": i14y_id
            }
        }
        """
        dataset_status_identifier_id_map = {
            "created": {},
            "updated": {},
            "unchanged": {},
            "deleted": {}
        }

        print("Fetching datasets from API...")

        datasets_bfs = self.fetch_datasets_from_api()

        print("\nStarting dataset import...\n")

        current_source_identifiers = {dataset["identifiers"][0] for dataset in datasets_bfs}
        all_existing_datasets = self.get_all_existing_datasets(self.organization)

        all_existing_datasets_identifier_id_map = self.get_all_identifier_id_map(all_existing_datasets)

        for dataset in datasets_bfs:
            identifier = dataset["identifiers"][0]
            print(f"\nProcessing dataset: {identifier}")
            print(f"Issued date: {dataset.get('issued')}")
            print(f"Modified date: {dataset.get('modified')}")

            modified_date = self.parse_date(dataset.get("modified"))
            created_date = self.parse_date(dataset.get("issued", dataset.get("modified")))

            is_new_dataset = identifier not in all_existing_datasets_identifier_id_map.keys()
            is_updated_dataset = modified_date and modified_date > yesterday

            existing_dataset_id = (
                all_existing_datasets_identifier_id_map[identifier]
                if identifier in all_existing_datasets_identifier_id_map.keys()
                else None
            )

            if existing_dataset_id and not is_updated_dataset:
                dataset_status_identifier_id_map["unchanged"][identifier] = existing_dataset_id
                print(f"No changes detected for dataset: {identifier} (already exists)\n")

            elif is_new_dataset or is_updated_dataset:
                action = "created" if is_new_dataset else "updated"
                print(f"{action.capitalize()} dataset detected: {identifier}")

                payload = self.create_dataset_payload(dataset)
                response_id, action = self.submit_to_api(
                    payload, identifier, all_existing_datasets_identifier_id_map
                )
                response_id = response_id.strip('"')

                if action == "created":
                    dataset_status_identifier_id_map["created"][identifier] = response_id
                    self.change_level_i14y(response_id, "Public")
                    self.change_status_i14y(response_id, "Recorded")

                elif action == "updated":
                    dataset_status_identifier_id_map["updated"][identifier] = response_id

                print(f"Success - Dataset {action}: {response_id}\n")

        # Find datasets that exist in previous_ids but not in current source
        datasets_to_delete = set(all_existing_datasets_identifier_id_map.keys()) - current_source_identifiers

        for identifier in datasets_to_delete:

            dataset_id = all_existing_datasets_identifier_id_map[identifier]

            try:
                self.change_level_i14y(dataset_id, "Internal")
                print(f"Changed publication level to Internal for {identifier}")
            except requests.HTTPError as e:
                print(f"Error changing publication level for {identifier}: {e.response.text}")
                # TODO: if Status 405 means that the resource has already Internal publication level, it would be better to check status and not error message
                if "The resource already has its publication level set to" not in str(e.response.text):
                    raise requests.HTTPError(e)

            response = self.delete_i14y(dataset_id)
            if response and response.status_code in {200, 204}:
                dataset_status_identifier_id_map["deleted"][identifier] = dataset_id
                print(f"Successfully deleted dataset: {identifier}")
            else:
                print(f"Failed to delete dataset {identifier}: {response.status_code} - {response.text}")

        log = f"Harvest completed successfully at {datetime.datetime.now()}\n"
        for action in ["created", "updated", "unchanged", "deleted"]:
            log += f"\n{action.capitalize()} datasets: {len(dataset_status_identifier_id_map[action])}"
            for bfs_identifier, i14y_id in dataset_status_identifier_id_map[action].items():
                log += f"\n- {bfs_identifier} : {i14y_id}"

        # Save log in root directory
        log_path = os.path.join(os.getcwd(), "harvest_log.txt")
        with open(log_path, "w") as f:
            f.write(log)

        print("\n=== Import Summary ===")
        print(f"Total processed: {len(datasets_bfs)}")
        for action in ["created", "updated", "unchanged", "deleted"]:
            print(f"Total {action.capitalize()}: {len(dataset_status_identifier_id_map[action])}")

        print(f"Log saved to: {log_path}")

        self.save_data(dataset_status_identifier_id_map, self.datasets_file_path)


if __name__ == "__main__":
    # We use the same file for ABN and PROD, therefore we can use env vars passed by github actions to distinguish one from another
    api_params = {
        "client_key": os.environ["CLIENT_KEY"],
        "client_secret": os.environ["CLIENT_SECRET"],
        "api_get_token_url": os.environ["GET_TOKEN_URL"],
        "api_base_url": os.environ["API_BASE_URL"],
        "organization_id": ORGANIZATION_ID,
    }

    harvester = HarvesterOFS(api_params)
    harvester.harvest()

    import_structures = os.environ.get("IMPORT_STRUCTURES", "False") == "True"
    # If ABN, we import the structures (not in prod for now)
    if import_structures:
        StructureImporter.execute(api_params)
