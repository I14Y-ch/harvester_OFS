import json
import os
from time import time
from typing import Any, Dict
import requests
import re
from config import I14Y_USER_AGENT


def reauth_if_token_expired(func):
    """Decorator to reauth before rerunning function if token is expired"""

    def wrap_func(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except requests.HTTPError as e:
            url = getattr(e.request, "url", "Unknown url")
            print(url)
            print(f"API error: {e.response.status_code} - {e.response.text}")
            if e.response.status_code == 401:
                self.api_token = self.get_access_token()
            return func(self, *args, **kwargs)

    return wrap_func


def timer(func):
    """Decorator that shows the execution time of the function object passed"""

    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f"Function {func.__name__!r} executed in {(t2-t1):.4f}s")
        return result

    return wrap_func


class CommonI14YAPI:
    """
    Shared functionnalities between multiple classes for I14Y token management and common API calls
    """

    def __init__(self, api_params: Dict[str, str]):
        """
        api_params must be a dict containing:
        - client_key: client key to generate token
        - client_secret: client secret to generate token
        - api_get_token_url: url to generate token
        - api_base_url: url for i14y api calls
        - organization: i14y organization
        """
        try:
            self.api_base_url = api_params["api_base_url"]
            self.organization = api_params["organization_id"]
            self.api_get_token_url = api_params["api_get_token_url"]
            self.client_key = api_params["client_key"]
            self.client_secret = api_params["client_secret"]
            self.api_token = self.get_access_token()
            self.bfs_identifier_pattern = re.compile(r"^\d+(-[a-z]+)?@bundesamt-fur-statistik-bfs$")
            self.datasets_file_path = os.path.join(os.getcwd(), "OGD_OFS", "data", "datasets.json")
        except (KeyError, TypeError):
            exception_str = "You need to provide the following parameters in a dict:"
            exception_str += "\n- client_key: client key to generate token"
            exception_str += "\n- client_secret: client secret to generate token"
            exception_str += "\n- api_get_token_url: url to generate token"
            exception_str += "\n- api_base_url: url for i14y api calls"
            exception_str += "\n- organization: i14y organization"
            raise Exception(exception_str)

    def get_access_token(self):
        """Generated an access token from client key and client secret"""
        data = {"grant_type": "client_credentials"}
        response = requests.post(
            self.api_get_token_url,
            data=data,
            verify=False,
            auth=(self.client_key, self.client_secret),
        )
        if response.status_code >= 400:
            raise Exception("Failed to get token")
        return "Bearer " + response.json()["access_token"]

    @reauth_if_token_expired
    def get_all_existing_datasets(self, publisherIdentifier: str, pageSize: int = 25) -> str:
        """Gets all existing datasets in one request"""

        print(f"Fetching all existing datasets from I14Y for organization {publisherIdentifier}...")

        all_datasets = []

        url = f"{self.api_base_url}/datasets"
        headers = {"Authorization": self.api_token, "Accept": "application/json", "User-Agent": I14Y_USER_AGENT}
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
            for dataset in data["data"]:
                identifier = dataset["identifiers"][0]
                if self.bfs_identifier_pattern.match(identifier):
                    all_datasets.append(dataset)

            i += 1
            has_more = len(data["data"]) > 0

        return all_datasets

    def save_data(self, data: Dict[str, Any], file_path: str) -> None:
        """Saves data to a JSON file."""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        try:
            with open(file_path, "w") as file:
                json.dump(data, file)
        except IOError as e:
            print(f"Error saving data to {file_path}: {e}")

    def load_data(self, file_path: str) -> Dict[str, Any]:
        """Loads data from a JSON file."""
        if not os.path.exists(file_path):
            print(f"Warning: {file_path} does not exist.")
            return {}

        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from {file_path}: {e}")
            return {}
        except IOError as e:
            print(f"Error loading data from {file_path}: {e}")
            return {}
