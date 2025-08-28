# Workflow Example
    # Receive a dataset distribution with a PX file URL
    # Check if it matches PX file patterns
    # Download and parse the PX file metadata
    # Create a SHACL graph 
    # Upload the structure

import re
import requests
from rdflib import Graph, Namespace, RDF, URIRef, Literal
from rdflib.namespace import SH, RDFS, XSD, DCTERMS, QB, OWL
from urllib.parse import urlparse
import os
from datetime import datetime
from typing import List, Dict

class StructureImporter:
    YEAR_KEYWORDS = {
        'en': ['year', 'yr'],
        'de': ['jahr', 'jahrgang'],
        'fr': ['année', 'an'],
        'it': ['anno', 'annata']
    }
    
    def __init__(self, api_token: str):
        self.g = Graph()
        self.api_token = api_token
        self._init_namespaces()
        
    def _init_namespaces(self):
        """Initialize all required namespaces"""
        self.SH = SH
        self.QB = QB
        self.DCTERMS = DCTERMS
        self.schema = Namespace("https://schema.org/")
        self.pav = Namespace("http://purl.org/pav/")
        self.rdfs = RDFS
        self.OWL = OWL
        self.XSD = XSD
        
        self.g.bind("sh", self.SH)
        self.g.bind("qb", self.QB)
        self.g.bind("dcterms", self.DCTERMS)
        self.g.bind("schema", self.schema)
        self.g.bind("pav", self.pav)
        self.g.bind("rdfs", self.rdfs)
        self.g.bind("owl", self.OWL)
        self.g.bind("xsd", self.XSD)

# PX File Processing
    # extract_px_identifier(): Extracts identifiers from BFS URLs (like "px-x-1304030000_301")
    # download_px_file(): Downloads PX files directly from the BFS download API
    # is_px_distribution(): Identifies whether a data distribution is a PX file based on URL patterns
    
# Metadata Extraction
# The parse_px_metadata() method extracts key information from PX files:
    # Titles and descriptions in multiple languages
    # Dimensions: STUB dimensions (row categories) and HEADING dimensions (column categories)
    # Values: The actual categorical values for each dimension
    # Uses regex patterns to parse the PX format syntax
    
# SHACL Graph Creation
    # Converts the parsed metadata into a SHACL NodeShape that includes:

# API Integration
    # Uploads the generated SHACL structure to the API in Turtle format.

    
    def extract_px_identifier(self, url: str) -> str:
        """Extracts px identifier from URL"""
        path = urlparse(url).path
        basename = os.path.basename(path)
        
        if '.' in basename:
            basename = basename.split('.')[0]
        
        if re.match(r'px-x-\d+_\d+', basename.lower()):
            return basename
        return None

    def download_px_file(self, identifier: str) -> str:
        """Downloads PX file using BFS download API"""
        download_url = f"https://www.pxweb.bfs.admin.ch/DownloadFile.aspx?file={identifier}"
        resp = requests.get(download_url)
        resp.raise_for_status()
        return resp.text

    def clean_dimension_name(self, name: str) -> str:
        """Cleans dimension names by replacing special chars with single underscores"""
        name = re.sub(r'[/ ]+', '_', name)
        name = re.sub(r'^_+|_+$', '', name)
        name = re.sub(r'_+', '_', name)
        return name

    def _is_year_column(self, column_name: str) -> bool:
        """Checks if column name indicates a year dimension"""
        lower_name = column_name.lower()
        for keywords in self.YEAR_KEYWORDS.values():
            if any(keyword in lower_name for keyword in keywords):
                return True
        return False
    
    def _is_valid_date(self, value: str) -> bool:
        """Checks if value is in YYYY-MM-DD format"""
        parts = value.split('-')
        return (len(parts) == 3 and 
                len(parts[0]) == 4 and 
                parts[0].isdigit() and
                parts[1].isdigit() and 
                parts[2].isdigit())
        
# data types for each column:
    # Detects year/date columns by looking for keywords like "year", "jahr", "année"
    # Identifies integers, decimals, booleans, and dates
    # Adds numeric constraints (min/max values) for numeric fields
    # Defaults to string type when uncertain
    def _guess_property_type(self, values: List[str], column_name: str) -> URIRef:
        """Infers the most specific datatype from values"""
        if not values:
            return self.XSD.string
            
        sample = values[0].strip() if values[0] else ""

        # Check for year/dates first
        if self._is_year_column(column_name):
            if (len(sample) == 4 and sample.isdigit()) or self._is_valid_date(sample):
                return self.XSD.date
            
        # Check for integers
        if all(v.strip().isdigit() for v in values if v.strip()):
            return self.XSD.integer
            
        # Check for decimals
        decimal_count = 0
        for v in values:
            if v.strip():
                try:
                    float(v)
                    decimal_count += 1
                except ValueError:
                    pass
        if decimal_count == len([v for v in values if v.strip()]):
            return self.XSD.decimal
            
        # Check for booleans
        bool_values = {'true', 'false', 't', 'f', 'yes', 'no', '1', '0'}
        if all(v.strip().lower() in bool_values for v in values if v.strip()):
            return self.XSD.boolean
            
        # Check for dates
        if all(self._is_valid_date(v.strip()) for v in values if v.strip()):
            return self.XSD.date
            
        # Default to string
        return self.XSD.string
    
    def _add_numeric_constraints(self, prop_uri: URIRef, values: List[str], datatype: URIRef):
        """Adds min/max constraints for numeric properties"""
        numeric_values = []
        for v in values:
            if v.strip():
                try:
                    num_val = float(v)
                    if datatype == self.XSD.integer and num_val.is_integer():
                        numeric_values.append(int(num_val))
                    else:
                        numeric_values.append(num_val)
                except ValueError:
                    continue
        
        if numeric_values:
            min_val = min(numeric_values)
            max_val = max(numeric_values)
            
            self.g.add((prop_uri, SH.minInclusive, Literal(min_val, datatype=datatype)))
            self.g.add((prop_uri, SH.maxInclusive, Literal(max_val, datatype=datatype)))

    def parse_px_metadata(self, px_content: str) -> Dict:
        """Parses PX file and extracts metadata including titles, descriptions and values"""
        metadata = {
            "titles": {},
            "descriptions": {},
            "dimensions": [],  # For STUB dimensions
            "heading": {"labels": {}},  # Single HEADING dimension
            "values": {}  # Stores VALUES for each dimension
        }
        
        # Patterns for different metadata fields
        stub_pattern = re.compile(r'STUB(?:\[(\w+)\])?="(.*?)";')
        heading_pattern = re.compile(r'HEADING(?:\[(\w+)\])?="(.*?)";')
        title_pattern = re.compile(r'TITLE(?:\[(\w+)\])?="(.*?)";')
        desc_pattern = re.compile(r'DESCRIPTION(?:\[(\w+)\])?="(.*?)";')
        values_pattern = re.compile(r'VALUES(?:\[(\w+)\])?\("(.+?)"\)="(.*?)";')
        
        for line in px_content.splitlines():
            line = line.strip()
            
            # Handle TITLE
            if line.startswith("TITLE"):
                m = title_pattern.match(line)
                if m:
                    lang = m.group(1) or "de"
                    metadata["titles"][lang] = m.group(2)
            
            # Handle DESCRIPTION
            elif line.startswith("DESCRIPTION"):
                m = desc_pattern.match(line)
                if m:
                    lang = m.group(1) or "de"
                    metadata["descriptions"][lang] = m.group(2)
            
            # Handle STUB dimensions
            elif line.startswith("STUB"):
                m = stub_pattern.match(line)
                if m:
                    lang = m.group(1) or "de"
                    dimensions = [d.strip() for d in m.group(2).split('","')]
                    
                    for i, dim in enumerate(dimensions):
                        if i >= len(metadata["dimensions"]):
                            metadata["dimensions"].append({"labels": {}})
                        metadata["dimensions"][i]["labels"][lang] = dim
            
            # Handle HEADING dimensions
            elif line.startswith("HEADING"):
                m = heading_pattern.match(line)
                if m:
                    lang = m.group(1) or "de"
                    dimension = m.group(2).strip()
                    metadata["heading"]["labels"][lang] = dimension
            
            # Handle VALUES
            elif line.startswith("VALUES"):
                m = values_pattern.match(line)
                if m:
                    lang = m.group(1) or "de"
                    dim_name = m.group(2)
                    values = [v.strip().strip('"') for v in m.group(3).split(",")]
                    
                    clean_dim = self.clean_dimension_name(dim_name)
                    if clean_dim not in metadata["values"]:
                        metadata["values"][clean_dim] = {}
                    metadata["values"][clean_dim][lang] = values
        
        return metadata

    def get_preferred_name(self, dim_labels: Dict, preferred_langs: List = ["en", "de"]) -> str:
        """Get the preferred name for URI (English first, then German)"""
        for lang in preferred_langs:
            if lang in dim_labels:
                return self.clean_dimension_name(dim_labels[lang])
        return self.clean_dimension_name(list(dim_labels.values())[0])

    def create_shacl_graph(self, dataset_id: str, structure_name: str, metadata: Dict) -> Graph:
        """Creates SHACL graph with multilingual properties and proper datatypes"""
        # Create NodeShape with titles and descriptions
        structure_uri = URIRef(f"https://www.i14y.admin.ch/resources/datasets/{dataset_id}/structure/{structure_name}")
        self.g.add((structure_uri, RDF.type, self.SH.NodeShape))
        
        # Add multilingual titles
        for lang, title in metadata["titles"].items():
            self.g.add((structure_uri, self.rdfs.label, Literal(title, lang=lang)))
        
        # Add multilingual descriptions
        for lang, desc in metadata["descriptions"].items():
            self.g.add((structure_uri, self.DCTERMS.description, Literal(desc, lang=lang)))
        
        # Create PropertyShapes for STUB dimensions
        for dim in metadata["dimensions"]:
            if not dim.get("labels"):
                continue
                
            preferred_name = self.get_preferred_name(dim["labels"])
            prop_uri = URIRef(f"https://www.i14y.admin.ch/resources/datasets/{dataset_id}/structure/{structure_name}/{preferred_name}")
            
            self.g.add((prop_uri, RDF.type, self.SH.PropertyShape))
            self.g.add((structure_uri, self.SH.property, prop_uri))
            self.g.add((prop_uri, self.SH.path, Literal(preferred_name)))
            
            # Add all language labels
            for lang, label in dim["labels"].items():
                self.g.add((prop_uri, self.rdfs.label, Literal(label, lang=lang)))
            
            # Infer datatype from VALUES if available
            if preferred_name in metadata["values"]:
                values = next(iter(metadata["values"][preferred_name].values()))  # Get first language's values
                column_name = next(iter(dim["labels"].values()))  # Get first language's label
                datatype = self._guess_property_type(values, column_name)
                
                # Add numeric constraints if applicable
                if datatype in [self.XSD.integer, self.XSD.decimal]:
                    self._add_numeric_constraints(prop_uri, values, datatype) 
                    self.g.add((prop_uri, self.SH.datatype, datatype))
        
        # Create PropertyShape for HEADING dimension
        if metadata["heading"]["labels"]:
            preferred_name = self.get_preferred_name(metadata["heading"]["labels"])
            prop_uri = URIRef(f"https://www.i14y.admin.ch/resources/datasets/{dataset_id}/structure/{structure_name}/{preferred_name}")
            
            self.g.add((prop_uri, RDF.type, self.SH.PropertyShape))
            self.g.add((structure_uri, self.SH.property, prop_uri))
            self.g.add((prop_uri, self.SH.path, Literal(preferred_name)))
            
            # Add all language labels for HEADING
            for lang, label in metadata["heading"]["labels"].items():
                self.g.add((prop_uri, self.rdfs.label, Literal(label, lang=lang)))
            
            # Infer datatype from VALUES if available
            if preferred_name in metadata["values"]:
                values = next(iter(metadata["values"][preferred_name].values()))  # Get first language's values
                column_name = next(iter(metadata["heading"]["labels"].values()))  # Get first language's label
                datatype = self._guess_property_type(values, column_name)
                
                # Add numeric constraints if applicable
                if datatype in [self.XSD.integer, self.XSD.decimal]:
                    self._add_numeric_constraints(prop_uri, values, datatype)
            else:
                datatype = self.XSD.string  # Default
            
            self.g.add((prop_uri, self.SH.datatype, datatype))
        
        return self.g

    def upload_structure(self, dataset_id: str, structure_graph: Graph) -> bool:
        """Uploads the SHACL structure to the API"""
        headers = {
            "Authorization": self.api_token,
            "Content-Type": "text/turtle"
        }
        
        url = f"https://api-a.i14y.admin.ch/api/partner/v1/datasets/{dataset_id}/structures/imports"
        
        try:
            response = requests.post(
                url,
                headers=headers,
                data=structure_graph.serialize(format="turtle"),
                timeout=30
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error uploading structure for dataset {dataset_id}: {str(e)}")
            return False

    def is_px_distribution(self, distribution: dict) -> bool:
        """
        Checks if a distribution is a valid PX file based on its access URL pattern.
        
        Args:
            distribution: A dictionary containing distribution metadata
            
        Returns:
            bool: True if the distribution appears to be a PX file, False otherwise
        """
        # Get the access URL from different possible locations in the distribution
        access_url = None
        if isinstance(distribution.get('accessUrl'), dict):
            access_url = distribution['accessUrl'].get('uri')
        elif isinstance(distribution.get('downloadUrl'), dict):
            access_url = distribution['downloadUrl'].get('uri')
        else:
            access_url = distribution.get('accessUrl') or distribution.get('downloadUrl')
        
        if not access_url or not isinstance(access_url, str):
            return False
        
        # Normalize the URL by removing query parameters and fragments
        clean_url = access_url.split('?')[0].split('#')[0]
        
        # Check for PX patterns in the URL
        px_patterns = [
            r'px-x-\d+_\d+$',                     # Example: px-x-1304030000_301
            r'px-x-\d+_\d+\.px$',                 # Example: px-x-1103020200_101.px
            r'/[^/]*px-x-\d+_\d+[^/]*$',          # In path segments
        ]
        
        # Check all patterns
        for pattern in px_patterns:
            if re.search(pattern, clean_url, re.IGNORECASE):
                return True
        
        return False
        
    def process_px_distribution(self, distribution: Dict, dataset_id: str) -> bool:
        """Processes a distribution to extract PX structure if available"""
        print(f"Processing PX distribution for dataset ID: {dataset_id}") 
        # First check if this looks like a PX distribution
        if not self.is_px_distribution(distribution):
            return False
            
        access_url = None
        if isinstance(distribution.get('accessUrl'), dict):
            access_url = distribution['accessUrl'].get('uri')
        elif isinstance(distribution.get('downloadUrl'), dict):
            access_url = distribution['downloadUrl'].get('uri')
        else:
            access_url = distribution.get('accessUrl') or distribution.get('downloadUrl')
        
        if not access_url:
            return False
            
        # Extract identifier
        identifier = self.extract_px_identifier(access_url)
        if not identifier:
            return False
            
        try:
            px_content = self.download_px_file(identifier)
            metadata = self.parse_px_metadata(px_content)
            structure_graph = self.create_shacl_graph(dataset_id, identifier, metadata)
            return self.upload_structure(dataset_id, structure_graph)
        except Exception as e:
            print(f"Error processing PX file {identifier} for dataset {dataset_id}: {str(e)}")
            return False


