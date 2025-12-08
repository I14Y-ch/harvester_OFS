"""
Extensible structure importer - works with any format importer
"""

import json
import os
import traceback
import requests
import urllib3
from common import CommonI14YAPI, reauth_if_token_expired
from datetime import datetime
from rdflib import Graph, Namespace, RDF, Literal
from rdflib.namespace import SH, RDFS, XSD, DCTERMS
from typing import Dict, List
from config import ORGANIZATION_ID
from format_importers import get_suitable_importer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class StructureImporter(CommonI14YAPI):
    """Main structure importer that works with any format"""

    @staticmethod
    def execute(api_params: Dict, import_all: bool = False):
        """Main execution"""
        # If import_all=True we import structures for all the datasets and not only those updated and created by the harvester (useful for first run)

        importer = StructureImporter(api_params)
        datasets_to_process = {}

        if not import_all:
            datasets_to_process = importer.create_datasets_to_process()

        importer.run_import(datasets_to_process, import_all=import_all)

    def __init__(self, api_params: Dict[str, str]):
        """
        api_params must be a dict containing:
        - client_key: client key to generate token
        - client_secret: client secret to generate token
        - api_get_token_url: url to generate token
        - api_base_url: url for i14y api calls
        - organization: i14y organization
        """
        super().__init__(api_params)
        self.identifier_dataset_map = {}

    def create_datasets_to_process(self) -> Dict[str, str]:
        """
        Create a dict to process based on datasets.json file.

        Returns:
            Dict[str,str]: Bfs identifier -> i14y id map
        """
        dataset_status_identifier_id_map = self.load_data(self.datasets_file_path)

        datasets_to_process = {}

        actions = ["created", "updated"]

        for action in actions:
            for bfs_identifier, i14y_id in dataset_status_identifier_id_map[action].items():
                datasets_to_process[bfs_identifier] = i14y_id

        return datasets_to_process

    def create_shacl_graph(self, metadata: Dict) -> str:
        """Create SHACL graph from metadata (format-agnostic)"""
        g = Graph()

        # Namespaces
        SH_NS = SH
        DCTERMS_NS = DCTERMS
        RDFS_NS = RDFS
        XSD_NS = XSD
        I14Y_NS = Namespace("https://www.i14y.admin.ch/resources/datasets/structure/")

        g.bind("sh", SH_NS)
        g.bind("dcterms", DCTERMS_NS)
        g.bind("rdfs", RDFS_NS)
        g.bind("xsd", XSD_NS)
        g.bind("i14y", I14Y_NS)

        # Create main shape
        shape_name = f"{metadata['identifier']}Shape"
        shape_uri = I14Y_NS[shape_name]

        g.add((shape_uri, RDF.type, SH_NS.NodeShape))

        # Add titles
        for lang, title in metadata["title"].items():
            g.add((shape_uri, RDFS_NS.label, Literal(title, lang=lang)))

        # Add descriptions
        for lang, desc in metadata["description"].items():
            g.add((shape_uri, DCTERMS_NS.description, Literal(desc, lang=lang)))

        # Add timestamps
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        g.add((shape_uri, DCTERMS_NS.created, Literal(now, datatype=XSD_NS.dateTime)))
        g.add((shape_uri, DCTERMS_NS.modified, Literal(now, datatype=XSD_NS.dateTime)))

        g.add((shape_uri, SH_NS.closed, Literal(True)))

        # Add properties
        for i, prop in enumerate(metadata["properties"]):
            prop_uri = I14Y_NS[f"{shape_name}/{prop['name']}"]

            g.add((prop_uri, RDF.type, SH_NS.PropertyShape))
            g.add((shape_uri, SH_NS.property, prop_uri))
            g.add((prop_uri, SH_NS.path, prop_uri))
            g.add((prop_uri, SH_NS.order, Literal(i)))
            g.add((prop_uri, SH_NS.minCount, Literal(1)))
            g.add((prop_uri, SH_NS.maxCount, Literal(1)))

            # Set datatype
            datatype_map = {
                "string": XSD_NS.string,
                "integer": XSD_NS.integer,
                "decimal": XSD_NS.decimal,
                "gYear": XSD_NS.gYear,
                "date": XSD_NS.date,
                "boolean": XSD_NS.boolean,
            }
            datatype = datatype_map.get(prop["datatype"], XSD_NS.string)
            g.add((prop_uri, SH_NS.datatype, datatype))

            # Add multilingual names
            for lang, label in prop["labels"].items():
                g.add((prop_uri, SH_NS.name, Literal(label, lang=lang)))

        return g.serialize(format="turtle")

    @reauth_if_token_expired
    def upload_structure(self, dataset_id: str, turtle_data: str) -> bool:
        """Upload SHACL structure to API"""
        headers = {
            "Authorization": self.api_token,
            # Remove Content-Type header; requests will set it automatically for multipart/form-data
        }

        url = f"{self.api_base_url}/datasets/{dataset_id}/structures/imports"

        # Prepare the file for multipart upload
        files = {"file": ("structure.ttl", turtle_data, "text/turtle")}

        try:
            print(f"Uploading structure to {url}...")
            response = requests.post(url, headers=headers, files=files, verify=False, timeout=30)

            if response.status_code in {200, 201, 204}:
                print(f"\tStructure uploaded: {response.text.strip()}")
                return True
            else:
                print(f"\tUpload failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"\tUpload error: {str(e)}")
            return False

    @reauth_if_token_expired
    def delete_structure(self, dataset_id: str) -> bool:
        """Delete existing structure"""
        headers = {"Authorization": self.api_token, "Content-Type": "application/json"}

        url = f"{self.api_base_url}/datasets/{dataset_id}/structures"

        try:
            response = requests.delete(url, headers=headers, verify=False, timeout=30)
            if response.status_code in {200, 204}:
                print(f"Structure for dataset {dataset_id} deleted successfully.")
                return True
            elif response.status_code == 404:
                print(f"Structure for dataset {dataset_id} not found (already deleted or does not exist).")
                return True
            else:
                print(f"Failed to delete structure for dataset {dataset_id}: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Error deleting structure for dataset {dataset_id}: {str(e)}")
            return False

    def build_identifier_dataset_map(self) -> Dict:
        """Builds a id->dataset map with fetched datasets for current organization"""
        identifier_dataset_map = {}
        all_existing_datasets = self.get_all_existing_datasets(self.organization)
        for dataset in all_existing_datasets:
            bfs_identifier = dataset["identifiers"][0]
            if self.bfs_identifier_pattern.match(bfs_identifier):
                identifier_dataset_map[bfs_identifier] = dataset
        return identifier_dataset_map

    def find_processable_distributions(self, distributions: List[Dict]) -> List[tuple]:
        """Find distributions that can be processed and deduplicate"""
        processable = []
        seen_identifiers = set()

        # print(f"DEBUG distributions: {distributions}")

        for dist in distributions:
            importer, format_name = get_suitable_importer(dist)

            # print(f"DEBUG importer: {importer}, format_name: {format_name}")

            if importer:
                identifier = importer.get_identifier(dist)
                print(f"identifier: {identifier}")
                print(f"Identifier type: {type(identifier)}, value: {identifier}")

                # Ensure identifier is a string
                if isinstance(identifier, str):
                    identifier_lower = identifier.lower()
                    if identifier_lower not in seen_identifiers:
                        processable.append((dist, importer, format_name, identifier))
                        seen_identifiers.add(identifier_lower)
                    else:
                        print(f"\tSkipping duplicate {format_name} file: {identifier}")
                else:
                    print(f"\tInvalid identifier (not a string): {identifier}")
        return processable

    def process_dataset(self, dataset_id: str, identifier: str) -> bool:
        """Process a single dataset and create structure"""
        print(f"Processing: {identifier}")

        # Get dataset from API
        dataset_data = self.identifier_dataset_map[identifier]
        if not dataset_data:
            return False

        distributions = dataset_data.get("distributions", [])
        if not distributions:
            print(f"\tNo distributions found")
            return False

        # Find processable distributions (with deduplication)
        processable = self.find_processable_distributions(distributions)
        if not processable:
            print(f"\tNo supported file formats found")
            return False

        # Process first suitable distribution
        dist, importer, format_name, file_id = processable[0]

        if format_name == "csv" and len(processable) > 1:
            print(f"\tMore than 1 csv file detected, skipping file_id: {file_id}")
            return False

        print(f"\tProcessing {format_name} file: {file_id}")

        # Always delete existing structure for updated datasets, optional for new ones
        # For new datasets, try to delete in case there's an old structure
        print(f"\tDeleting existing structure (dataset was updated)")
        self.delete_structure(dataset_id)

        # Download and parse file
        metadata = importer.download_and_parse(dist)

        # Create and upload SHACL
        turtle_data = self.create_shacl_graph(metadata)
        success = self.upload_structure(dataset_id, turtle_data)

        if success:
            print(f"\tStructure created successfully")
            return True
        else:
            return False

    def run_import(self, datasets_to_process: Dict[str, str], import_all: bool = False):
        """
        Main import process with harvest log awareness.

        Args:
            datasets_to_process Dict[str,str]: Bfs identifier -> i14y id map for datasets to process (those created or updated by harvester)
            import_all (bool):  if True we import structures for all the datasets and not only those updated and created by the harvester (useful for first run)
                                if False we import structures only for datasets updated or created by the harvester
        """
        # Statistics
        created_structure_datasets = []
        skipped_structure_datasets = []
        error_structure_datasets = []

        print("Starting extensible structure import...")
        self.identifier_dataset_map = self.build_identifier_dataset_map()

        dataset_to_process_identifier_data_map = {}

        if import_all:
            dataset_to_process_identifier_data_map = self.identifier_dataset_map
        else:
            for bfs_identifier, _ in datasets_to_process.items():
                dataset_to_process_identifier_data_map[bfs_identifier] = self.identifier_dataset_map[bfs_identifier]

        print(f"Datasets to process: {len(dataset_to_process_identifier_data_map)}")

        # Process datasets
        for bfs_identifier, data in dataset_to_process_identifier_data_map.items():
            dataset_id = data.get("id")
            if not dataset_id:
                continue

            print(f"Processing dataset: {bfs_identifier}")

            if self.process_dataset(dataset_id, bfs_identifier):
                created_structure_datasets.append(f"{bfs_identifier} : {dataset_id}")
            else:
                skipped_structure_datasets.append(f"{bfs_identifier} : {dataset_id}")

        created_structures = len(created_structure_datasets)
        skipped = len(skipped_structure_datasets)
        errors = len(error_structure_datasets)

        # Print summary
        print(f"\n=== Summary ===")
        print(f"Structures created: {created_structures}")
        print(f"Skipped: {skipped}")
        print(f"Errors: {errors}")

        # Save log
        log_content = f"Structure import completed at {datetime.now()}"
        log_content += f"\nResults:\n"
        log_content += f"\nStructures created: {created_structures}"
        for x in created_structure_datasets:
            log_content += f"\n- {x}"
        log_content += f"\nSkipped: {skipped}"
        for x in skipped_structure_datasets:
            log_content += f"\n- {x}"
        log_content += f"\nErrors: {errors}"
        for x in error_structure_datasets:
            log_content += f"\n- {x}"

        with open("structure_import_log.txt", "w") as f:
            f.write(log_content)

        print("Log saved to structure_import_log.txt")


if __name__ == "__main__":
    api_params = {
        "client_key": os.environ["CLIENT_KEY"],
        "client_secret": os.environ["CLIENT_SECRET"],
        "api_get_token_url": os.environ["GET_TOKEN_URL"],
        "api_base_url": os.environ["API_BASE_URL"],
        "organization_id": ORGANIZATION_ID,
    }

    import_all = os.environ.get("IMPORT_ALL", "False") == "True"

    StructureImporter.execute(api_params, import_all=import_all)
