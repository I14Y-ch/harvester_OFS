"""
Extensible structure importer - works with any format importer
"""

import json
import os
import time
import requests
from config_structures import DATASET_IDS_PATH, USE_DATASET_LIST  # Explicitly import required variables
from datetime import datetime
from rdflib import Graph, Namespace, RDF, URIRef, Literal
from rdflib.namespace import SH, RDFS, XSD, DCTERMS
from typing import Dict, List
from format_importers import get_suitable_importer


def create_datasets_to_process(harvest_log_path: str, dataset_ids_path: str) -> List[str]:
    """
    Create a list of dataset IDs to process based on the harvest log and dataset IDs.

    Args:
        harvest_log_path (str): Path to the harvest log file.
        dataset_ids_path (str): Path to the dataset IDs JSON file.

    Returns:
        List[str]: A list of dataset IDs to process.
    """
    # Load dataset IDs
    try:
        with open(dataset_ids_path, 'r') as f:
            dataset_ids = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {dataset_ids_path} not found")
        return []

    # Parse harvest log to understand dataset status
    datasets_to_process = []
    try:
        with open(harvest_log_path, 'r') as f:
            content = f.read()

        current_section = None
        for line in content.split('\n'):
            line = line.strip()

            if "Created datasets:" in line:
                current_section = "created"
            elif "Updated datasets:" in line:
                current_section = "updated"
            elif "Unchanged datasets:" in line:
                current_section = "unchanged"
            elif "Deleted datasets:" in line:
                current_section = "deleted"
            elif line.startswith("- ") and current_section:
                dataset_id = line[2:].strip()  # Remove "- " prefix
                datasets_to_process.append(dataset_id)

    except FileNotFoundError:
        print("No harvest log found - processing all datasets as new")
        datasets_to_process = list(dataset_ids.keys())

    return datasets_to_process


class StructureImporter:
    """Main structure importer that works with any format"""
    
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://api-a.i14y.admin.ch/api/partner/v1"
    
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
                "boolean": XSD_NS.boolean
            }
            datatype = datatype_map.get(prop["datatype"], XSD_NS.string)
            g.add((prop_uri, SH_NS.datatype, datatype))
            
            # Add multilingual names
            for lang, label in prop["labels"].items():
                g.add((prop_uri, SH_NS.name, Literal(label, lang=lang)))
        
        return g.serialize(format="turtle")
    
    def upload_structure(self, dataset_id: str, turtle_data: str) -> bool:
        """Upload SHACL structure to API"""
        headers = {
            "Authorization": self.api_token,
            # Remove Content-Type header; requests will set it automatically for multipart/form-data
        }
        
        url = f"{self.base_url}/datasets/{dataset_id}/structures/imports"
        
        # Prepare the file for multipart upload
        files = {
            "file": ("structure.ttl", turtle_data, "text/turtle")
        }
        
        try:
            print(f"Uploading structure to {url}...")  # Debugging: Print the URL
            #print(f"Headers: {headers}")
            #print(f"Files: {files}")
            response = requests.post(url, headers=headers, files=files, verify='src/local_testing/certificate_ABN.crt', timeout=30)
            
            if response.status_code in [200, 201, 204]:
                print(f"    Structure uploaded: {response.text.strip()}")
                return True
            else:
                print(f"    Upload failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"    Upload error: {str(e)}")
            return False
    
    def delete_structure(self, dataset_id: str) -> bool:
        """Delete existing structure"""
        headers = {
            "Authorization": self.api_token,
            "Content-Type": "application/json"
        }
        
        url = f"{self.base_url}/datasets/{dataset_id}/structures"
        
        try:
            response = requests.delete(url, headers=headers, verify='src/local_testing/certificate_ABN.crt', timeout=30)
            if response.status_code in [200, 204]:
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
    
    def get_dataset_from_api(self, dataset_id: str) -> Dict:
        """Fetch dataset from API"""
        headers = {
            "Authorization": self.api_token,
            "Accept": "application/json"
        }
        
        url = f"https://api-a.i14y.admin.ch/api/partner/v1/datasets/{dataset_id}"
        
        try:
            response = requests.get(url, headers=headers, verify='src/local_testing/certificate_ABN.crt', timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"  Could not fetch dataset: {response.status_code}")
                return {}
        except Exception as e:
            print(f"  Fetch error: {str(e)}")
            return {}
    
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
                        print(f"    Skipping duplicate {format_name} file: {identifier}")
                else:
                    print(f"    Invalid identifier (not a string): {identifier}")      
        return processable
    
    def process_dataset(self, dataset_id: str, identifier: str) -> bool:
        """Process a single dataset and create structure"""
        print(f"Processing: {identifier}")
        
        # Get dataset from API
        dataset_data = self.get_dataset_from_api(dataset_id)
        if not dataset_data:
            return False
            
        distributions = dataset_data.get('data', {}).get('distributions', [])
        if not distributions:
            print(f"  No distributions found")
            return False
        
        # Find processable distributions (with deduplication)
        processable = self.find_processable_distributions(distributions)
        if not processable:
            print(f"  No supported file formats found")
            return False
        
        # Process first suitable distribution
        dist, importer, format_name, file_id = processable[0]
        print(f"  Processing {format_name} file: {file_id}")
        
        try:
            # Always delete existing structure for updated datasets, optional for new ones
            # For new datasets, try to delete in case there's an old structure
            print(f"  Deleting existing structure (dataset was updated)")
            self.delete_structure(dataset_id)
            
            # Download and parse file
            metadata = importer.download_and_parse(dist)
            
            # Create and upload SHACL
            turtle_data = self.create_shacl_graph(metadata)
            success = self.upload_structure(dataset_id, turtle_data)
            
            if success:
                print(f"  Structure created successfully")
                return True
            else:
                return False
                
        except Exception as e:
            print(f"  Error processing {file_id}: {str(e)}")
            return False
    
    def run_import(self, datasets_to_process: Dict[str, Dict]):
        """
        Main import process with harvest log awareness.

        Args:
            datasets_to_process (Dict[str, Dict]): A dictionary of datasets to process, where the key is the dataset ID
                                                   and the value is the dataset metadata.
        """
        # Statistics
        processed = 0
        created_structures = 0
        skipped = 0
        errors = 0

        print("Starting extensible structure import...")
        print(f"Datasets to process: {len(datasets_to_process)}")

        # Process datasets
        for identifier, data in datasets_to_process.items():
            dataset_id = data.get('id')
            if not dataset_id:
                continue

            processed += 1

            try:
                print(f"Processing dataset: {identifier}")

                if self.process_dataset(dataset_id, identifier):
                    created_structures += 1

            except Exception as e:
                print(f"  Error: {str(e)}")
                errors += 1

            time.sleep(0.5)  # Rate limiting

        # Print summary
        print(f"\n=== Summary ===")
        print(f"Datasets processed: {processed}")
        print(f"Structures created: {created_structures}")
        print(f"Skipped: {skipped}")
        print(f"Errors: {errors}")

        # Save log
        log_content = f"Structure import completed at {datetime.now()}\n"
        log_content += f"Results:\n"
        log_content += f"- Datasets processed: {processed}\n"
        log_content += f"- Structures created: {created_structures}\n"
        log_content += f"- Skipped: {skipped}\n"
        log_content += f"- Errors: {errors}\n"

        with open('structure_import_log.txt', 'w') as f:
            f.write(log_content)

        print("Log saved to structure_import_log.txt")


def main():
    """Main execution"""
    #api_token = os.getenv('ACCESS_TOKEN')
    api_token = "xxxx"
    if not api_token:
        print("ERROR: ACCESS_TOKEN environment variable not set")
        return

    if USE_DATASET_LIST:
        # Get datasets from list
        try:
            with open(DATASET_IDS_PATH, 'r') as f:
                datasets_to_process = json.load(f)
        except FileNotFoundError:
            print(f"ERROR: {DATASET_IDS_PATH} not found")
            return
    else:
        # Create datasets to process from the harvest log
        dataset_ids_path_harvesting = 'OGD_OFS/data/dataset_ids.json'
        harvest_log_path = 'harvest_log.txt'

        try:
            with open(dataset_ids_path_harvesting, 'r') as f:
                dataset_ids = json.load(f)
        except FileNotFoundError:
            print(f"ERROR: {dataset_ids_path_harvesting} not found")
            return

        datasets_to_process_ids = create_datasets_to_process(harvest_log_path, dataset_ids_path_harvesting)

        # Build the datasets_to_process dictionary
        datasets_to_process = {}
        for ds_id in datasets_to_process_ids:
            if ds_id in dataset_ids:
                datasets_to_process[ds_id] = dataset_ids[ds_id]

    print(f"Datasets to process: {len(datasets_to_process)}")

    importer = StructureImporter(api_token)
    importer.run_import(datasets_to_process)


if __name__ == "__main__":
    main()