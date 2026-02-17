from concurrent.futures import ThreadPoolExecutor, as_completed
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

            headers = {"User-Agent": I14Y_USER_AGENT}

            params = {"skip": skip, "limit": limit}
            response = self.session.get(API_OFS_URL, params=params, verify=False, timeout=30, headers=headers)

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
        try:
            response = self.session.put(
                url=f"{self.api_base_url}/datasets/{id}/publication-level",
                params={"level": level},
                headers={
                    "Authorization": self.api_token,
                    "Content-Type": "application/json",
                    "Accept": "*/*",
                    "Accept-encoding": "json",
                    "User-Agent": I14Y_USER_AGENT,
                },
                verify=False,
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            txt = e.response.text if e.response is not None else str(e)
            if "The resource already has its publication level set to" not in str(txt):
                raise

    @reauth_if_token_expired
    def change_status_i14y(self, id, status):
        """Change registration status of a dataset in i14y"""
        response = self.session.put(
            url=f"{self.api_base_url}/datasets/{id}/registration-status",
            params={"status": status},
            headers={
                "Authorization": self.api_token,
                "Content-Type": "application/json",
                "Accept": "*/*",
                "Accept-encoding": "json",
                "User-Agent": I14Y_USER_AGENT,
            },
            verify=False,
        )
        response.raise_for_status()
        return response

    @reauth_if_token_expired
    def delete_i14y(self, dataset_id):
        headers = {
            "Authorization": self.api_token,
            "Content-Type": "application/json",
            "User-Agent": I14Y_USER_AGENT,
        }
        try:
            url_structures = f"{self.api_base_url}/datasets/{dataset_id}/structures"
            self.session.delete(url_structures, headers=headers, verify=False)
        except requests.HTTPError as e:
            print(f"Error trying to delete structure: {e.response.status_code} - {e.response.text}")
        url = f"{self.api_base_url}/datasets/{dataset_id}"
        response = self.session.delete(url, headers=headers, verify=False)
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
        headers = {
            "Authorization": self.api_token,
            "Content-Type": "application/json",
            "User-Agent": I14Y_USER_AGENT,
        }

        action = "created"
        if identifier and previous_ids and identifier in previous_ids.keys():
            dataset_id = previous_ids[identifier]
            url = f"{self.api_base_url}/datasets/{dataset_id}"
            response = self.session.put(url, json=payload, headers=headers)
            action = "updated"
        else:
            url = f"{self.api_base_url}/datasets"
            response = self.session.post(url, json=payload, headers=headers)

        if response.status_code not in {200, 201, 204}:
            response.raise_for_status()

        body = (response.text or "").strip()

        if action == "updated":
            returned_id = body.strip('"') if body else dataset_id
            return returned_id, action

        returned_id = body.strip('"') if body else dataset_id
        return returned_id, action

    def parse_date(self, date_str):
        """Safely parse a date string, returning None if invalid or missing"""
        if not date_str:
            return None
        try:
            return parser.parse(date_str)
        except (ValueError, TypeError):
            return None

    def _process_one_dataset(self, dataset, all_existing_map, yesterday):
        identifier = dataset["identifiers"][0]

        print(f"\nProcessing dataset: {identifier}")
        print(f"Issued date: {dataset.get('issued')}")
        print(f"Modified date: {dataset.get('modified')}")

        modified_date = self.parse_date(dataset.get("modified"))
        created_date = self.parse_date(dataset.get("issued", dataset.get("modified")))

        is_new_dataset = identifier not in all_existing_map.keys()
        is_updated_dataset = modified_date and modified_date > yesterday

        existing_dataset_id = all_existing_map[identifier] if identifier in all_existing_map.keys() else None

        if existing_dataset_id and not is_updated_dataset:
            return {"status": "unchanged", "identifier": identifier, "dataset_id": existing_dataset_id}

        elif is_new_dataset or is_updated_dataset:
            action = "created" if is_new_dataset else "updated"
            print(f"{action.capitalize()} dataset detected: {identifier}")

            payload = self.create_dataset_payload(dataset)
            response_id, action = self.submit_to_api(payload, identifier, all_existing_map)
            response_id = response_id.strip('"')

            if action == "created":
                self.change_level_i14y(response_id, "Public")
                self.change_status_i14y(response_id, "Recorded")

            print(f"Success - Dataset {action}: {response_id}\n")

            return {"status": action, "identifier": identifier, "dataset_id": response_id}

        return {"status": "skipped", "identifier": identifier, "dataset_id": None}

    def _delete_one_dataset(self, identifier, dataset_id):

        self.change_level_i14y(dataset_id, "Internal")
        print(f"Changed publication level to Internal for {identifier}")

        try:
            response = self.delete_i14y(dataset_id)
            print(f"Successfully deleted dataset: {identifier}")
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            txt = e.response.text if e.response is not None else str(e)
            print(f"Failed to delete dataset {identifier}: {code} - {txt}")
            raise

        return {"status": "deleted", "identifier": identifier, "dataset_id": dataset_id}

    def harvest(self):
        utc_plus_1 = datetime.timezone(datetime.timedelta(hours=1))
        now_utc_plus_1 = datetime.datetime.now(utc_plus_1)
        yesterday = now_utc_plus_1 - datetime.timedelta(days=1)

        dataset_status_identifier_id_map = {"created": {}, "updated": {}, "unchanged": {}, "deleted": {}}

        print("Fetching datasets from API...")
        datasets = self.fetch_datasets_from_api()
        print("\nStarting dataset import...\n")

        current_source_identifiers = {dataset["identifiers"][0] for dataset in datasets}
        all_existing_datasets = self.get_all_existing_datasets(self.organization)
        all_existing_datasets_identifier_id_map = self.get_all_identifier_id_map(all_existing_datasets)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self._process_one_dataset,
                    dataset,
                    all_existing_datasets_identifier_id_map,
                    yesterday,
                )
                for dataset in datasets
            ]

            for future in as_completed(futures):
                result = future.result()
                status = result["status"]
                identifier = result["identifier"]
                dataset_id = result["dataset_id"]

                if status in dataset_status_identifier_id_map and dataset_id:
                    dataset_status_identifier_id_map[status][identifier] = dataset_id

        datasets_to_delete = set(all_existing_datasets_identifier_id_map.keys()) - current_source_identifiers

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            delete_futures = [
                executor.submit(
                    self._delete_one_dataset,
                    identifier,
                    all_existing_datasets_identifier_id_map[identifier],
                )
                for identifier in datasets_to_delete
            ]

            for future in as_completed(delete_futures):
                result = future.result()
                identifier = result["identifier"]
                dataset_id = result["dataset_id"]
                dataset_status_identifier_id_map["deleted"][identifier] = dataset_id

        log = f"Harvest completed successfully at {datetime.datetime.now()}\n"
        for action in ["created", "updated", "unchanged", "deleted"]:
            log += f"\n{action.capitalize()} datasets: {len(dataset_status_identifier_id_map[action])}"
            for bfs_identifier, i14y_id in dataset_status_identifier_id_map[action].items():
                log += f"\n- {bfs_identifier} : {i14y_id}"

        log_path = os.path.join(os.getcwd(), "harvest_log.txt")
        with open(log_path, "w") as f:
            f.write(log)

        print("\n=== Import Summary ===")
        print(f"Total processed: {len(datasets)}")
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
    if import_structures:
        StructureImporter.execute(api_params)
