"""
Modular structure import framework for i14y datasets
Supports PX for now can be extended
"""

import abc
import requests
import json
import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import time

class StructureImporter(abc.ABC):
    """Abstract base class for structure importers"""
    
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://api-a.i14y.admin.ch/api/partner/v1"
    
    @abc.abstractmethod
    def can_process_distribution(self, distribution: Dict) -> bool:
        """Check if this importer can process the given distribution"""
        pass
    
    @abc.abstractmethod
    def process_distribution(self, distribution: Dict, dataset_id: str) -> Tuple[bool, Optional[str]]:
        """
        Process a distribution and create structure
        Returns: (success: bool, structure_id: Optional[str])
        """
        pass
    
    @abc.abstractmethod
    def get_importer_name(self) -> str:
        """Return the name of this importer for logging"""
        pass
    
    def delete_existing_structure(self, dataset_id: str) -> bool:
        """Delete existing structure for a dataset"""
        headers = {
            "Authorization": self.api_token,
            "Content-Type": "application/json"
        }
        
        url = f"{self.base_url}/datasets/{dataset_id}/structure"
        
        try:
            response = requests.delete(url, headers=headers, verify=False, timeout=30)
            if response.status_code in [200, 204, 404]:  # 404 means no structure exists
                return True
            else:
                print(f"Warning: Failed to delete structure for {dataset_id}: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error deleting structure for {dataset_id}: {str(e)}")
            return False
    
    def upload_structure(self, dataset_id: str, structure_data: str, content_type: str = "text/turtle") -> Tuple[bool, Optional[str]]:
        """Upload structure to the API"""
        headers = {
            "Authorization": self.api_token,
            "Content-Type": content_type
        }
        
        url = f"{self.base_url}/datasets/{dataset_id}/structures/imports"
        
        try:
            response = requests.post(
                url,
                headers=headers,
                data=structure_data,
                verify=False,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                structure_id = response.text.strip('"') if response.text else None
                return True, structure_id
            else:
                print(f"Error uploading structure for dataset {dataset_id}: {response.status_code} - {response.text}")
                return False, None
                
        except Exception as e:
            print(f"Error uploading structure for dataset {dataset_id}: {str(e)}")
            return False, None


class StructureImportManager:
    """Manages multiple structure importers and coordinates the import process"""
    
    def __init__(self, api_token: str, dataset_ids_file: str):
        self.api_token = api_token
        self.dataset_ids_file = dataset_ids_file
        self.importers: List[StructureImporter] = []
        self.stats = {
            'processed': 0,
            'structures_created': 0,
            'structures_updated': 0,
            'structures_deleted': 0,
            'errors': 0
        }
    
    def register_importer(self, importer: StructureImporter):
        """Register a structure importer"""
        self.importers.append(importer)
        print(f"Registered importer: {importer.get_importer_name()}")
    
    def load_dataset_ids(self) -> Dict[str, Dict]:
        """Load dataset IDs from file"""
        try:
            with open(self.dataset_ids_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Dataset IDs file not found: {self.dataset_ids_file}")
            return {}
    
    def get_dataset_from_api(self, dataset_id: str) -> Optional[Dict]:
        """Fetch dataset metadata from API"""
        headers = {
            "Authorization": self.api_token,
            "Accept": "application/json"
        }
        
        url = f"https://api-a.i14y.admin.ch/api/partner/v1/datasets/{dataset_id}"
        
        try:
            response = requests.get(url, headers=headers, verify=False, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching dataset {dataset_id}: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching dataset {dataset_id}: {str(e)}")
            return None
    
    def process_dataset(self, identifier: str, dataset_id: str, force_update: bool = False) -> bool:
        """Process a single dataset for structure import"""
        print(f"\nProcessing dataset: {identifier} (ID: {dataset_id})")
        
        # Get dataset metadata
        dataset_data = self.get_dataset_from_api(dataset_id)
        if not dataset_data:
            print(f"Could not fetch dataset data for {identifier}")
            return False
        
        # Get distributions
        distributions = dataset_data.get('data', {}).get('distributions', [])
        if not distributions:
            print(f"No distributions found for dataset {identifier}")
            return False
        
        # Group distributions by format/type to avoid duplicates
        processed_identifiers = set()
        structure_created = False
        
        # Try each distribution with each importer
        for dist_idx, distribution in enumerate(distributions):
            print(f"  Checking distribution {dist_idx + 1}/{len(distributions)}")
            
            for importer in self.importers:
                if importer.can_process_distribution(distribution):
                    # For PX files, check if we've already processed this PX identifier
                    if hasattr(importer, 'extract_px_identifier'):
                        access_url = self._get_access_url_from_distribution(distribution)
                        if access_url:
                            px_identifier = importer.extract_px_identifier(access_url)
                            if px_identifier and px_identifier in processed_identifiers:
                                print(f"  Skipping duplicate PX file: {px_identifier}")
                                continue
                            if px_identifier:
                                processed_identifiers.add(px_identifier)
                    
                    print(f"  Using {importer.get_importer_name()} for distribution {dist_idx + 1}")
                    
                    # Delete existing structure if force update or if we're about to create a new one
                    if force_update or not structure_created:
                        importer.delete_existing_structure(dataset_id)
                    
                    success, structure_id = importer.process_distribution(distribution, dataset_id)
                    
                    if success:
                        print(f"  Structure created successfully: {structure_id}")
                        structure_created = True
                        self.stats['structures_created'] += 1
                        break  # Move to next distribution
                    else:
                        print(f"  Failed to create structure with {importer.get_importer_name()}")
            
            if structure_created:
                break  # We found a working importer for this dataset
        
        if not structure_created:
            print(f"  No suitable importer found for dataset {identifier}")
        
        self.stats['processed'] += 1
        return structure_created
    
    def _get_access_url_from_distribution(self, distribution: Dict) -> Optional[str]:
        """Extract access URL from distribution for deduplication purposes"""
        if isinstance(distribution.get('accessUrl'), dict):
            return distribution['accessUrl'].get('uri')
        elif isinstance(distribution.get('downloadUrl'), dict):
            return distribution['downloadUrl'].get('uri')
        else:
            return distribution.get('accessUrl') or distribution.get('downloadUrl')
    
    def run_import(self, force_update: bool = False, specific_datasets: Optional[List[str]] = None):
        """Run the structure import process"""
        print("Starting structure import process...")
        print(f"Registered importers: {[imp.get_importer_name() for imp in self.importers]}")
        
        # Load dataset IDs
        dataset_ids = self.load_dataset_ids()
        if not dataset_ids:
            print("No dataset IDs found. Run the harvester first.")
            return
        
        # Filter datasets if specific ones requested
        if specific_datasets:
            dataset_ids = {k: v for k, v in dataset_ids.items() if k in specific_datasets}
            print(f"Processing specific datasets: {list(dataset_ids.keys())}")
        
        # Process each dataset
        for identifier, data in dataset_ids.items():
            dataset_id = data.get('id')
            if not dataset_id:
                print(f"No ID found for dataset {identifier}")
                continue
            
            try:
                self.process_dataset(identifier, dataset_id, force_update)
                time.sleep(0.5)  # Rate limiting
            except Exception as e:
                print(f"Error processing dataset {identifier}: {str(e)}")
                self.stats['errors'] += 1
        
        self.print_summary()
    
    def print_summary(self):
        """Print import summary"""
        print("\n=== Structure Import Summary ===")
        print(f"Datasets processed: {self.stats['processed']}")
        print(f"Structures created: {self.stats['structures_created']}")
        print(f"Structures updated: {self.stats['structures_updated']}")
        print(f"Structures deleted: {self.stats['structures_deleted']}")
        print(f"Errors: {self.stats['errors']}")
        
        # Create log
        log_content = f"Structure import completed at {datetime.now()}\n"
        log_content += f"Processed: {self.stats['processed']}\n"
        log_content += f"Created: {self.stats['structures_created']}\n"
        log_content += f"Updated: {self.stats['structures_updated']}\n"
        log_content += f"Deleted: {self.stats['structures_deleted']}\n"
        log_content += f"Errors: {self.stats['errors']}\n"
        
        # Save log
        log_path = 'structure_import_log.txt'
        with open(log_path, 'w') as f:
            f.write(log_content)
        print(f"Log saved to: {log_path}")


# Example usage and main execution
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Import structures for i14y datasets')
    parser.add_argument('--force-update', action='store_true', 
                       help='Force update all structures')
    parser.add_argument('--datasets', nargs='*', 
                       help='Process only specific datasets (by identifier)')
    
    args = parser.parse_args()
    
    # Get API token from environment
    api_token = os.getenv('ACCESS_TOKEN')
    if not api_token:
        print("ERROR: ACCESS_TOKEN environment variable not set")
        return
    
    # Initialize manager
    dataset_ids_file = 'OGD_OFS/data/dataset_ids.json'
    manager = StructureImportManager(api_token, dataset_ids_file)
    
    # Register importers
    # Import the simplified PX importer
    try:
        from simplified_px_shacl_importer import SimplifiedPXStructureImporter
        manager.register_importer(SimplifiedPXStructureImporter(api_token))
    except ImportError:
        print("Warning: PX Structure Importer not available")
    
    # Future importers can be registered here:
    # manager.register_importer(CSVStructureImporter(api_token))
    # manager.register_importer(JSONStructureImporter(api_token))
    
    # Run import
    manager.run_import(
        force_update=args.force_update,
        specific_datasets=args.datasets
    )


if __name__ == "__main__":
    main()
