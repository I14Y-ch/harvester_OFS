"""
Simplified PX to SHACL converter
"""

import re
import requests
import os
from urllib.parse import urlparse
from rdflib import Graph, Namespace, RDF, URIRef, Literal
from rdflib.namespace import SH, RDFS, XSD, DCTERMS
from typing import Dict, List, Optional, Tuple
from structure_import_framework import StructureImporter
from datetime import datetime


class SimplifiedPXStructureImporter(StructureImporter):
    """Simplified PX file structure importer with clean SHACL output"""
    
    def __init__(self, api_token: str):
        super().__init__(api_token)
        self.g = Graph()
        self._init_namespaces()
    
    def _init_namespaces(self):
        """Initialize namespaces with clean prefixes"""
        self.SH = SH
        self.DCTERMS = DCTERMS
        self.RDFS = RDFS
        self.XSD = XSD
        
        # Use simple, clean namespace
        self.i14y = Namespace("https://www.i14y.admin.ch/resources/datasets/structure/")
        
        self.g.bind("sh", self.SH)
        self.g.bind("dcterms", self.DCTERMS)
        self.g.bind("rdfs", self.RDFS)
        self.g.bind("xsd", self.XSD)
        self.g.bind("i14y", self.i14y)
    
    def get_importer_name(self) -> str:
        return "Simplified PX Structure Importer"
    
    def can_process_distribution(self, distribution: Dict) -> bool:
        """Check if this distribution is a PX file"""
        return self.is_px_distribution(distribution)
    
    def process_distribution(self, distribution: Dict, dataset_id: str) -> Tuple[bool, Optional[str]]:
        """Process PX distribution and create clean SHACL structure"""
        print(f"    Processing PX distribution for dataset ID: {dataset_id}")
        
        access_url = self._get_access_url(distribution)
        if not access_url:
            return False, None
        
        identifier = self.extract_px_identifier(access_url)
        if not identifier:
            return False, None
        
        try:
            # Download and parse PX file
            px_content = self.download_px_file(identifier)
            metadata = self.parse_px_metadata(px_content)
            
            # Create clean SHACL graph
            self.g = Graph()
            self._init_namespaces()
            self.create_clean_shacl_graph(dataset_id, identifier, metadata)
            
            # Upload structure
            turtle_data = self.g.serialize(format="turtle")
            success, structure_id = self.upload_structure(dataset_id, turtle_data, "text/turtle")
            
            return success, structure_id
            
        except Exception as e:
            print(f"    Error processing PX file {identifier}: {str(e)}")
            return False, None
    
    def _get_access_url(self, distribution: Dict) -> Optional[str]:
        """Extract access URL from distribution"""
        if isinstance(distribution.get('accessUrl'), dict):
            return distribution['accessUrl'].get('uri')
        elif isinstance(distribution.get('downloadUrl'), dict):
            return distribution['downloadUrl'].get('uri')
        else:
            return distribution.get('accessUrl') or distribution.get('downloadUrl')
    
    def extract_px_identifier(self, url: str) -> str:
        """Extract PX identifier from URL"""
        path = urlparse(url).path
        basename = os.path.basename(path)
        
        if '.' in basename:
            basename = basename.split('.')[0]
        
        if re.match(r'px-x-\d+_\d+', basename.lower()):
            return basename
        return None
    
    def download_px_file(self, identifier: str) -> str:
        """Download PX file from BFS"""
        download_url = f"https://www.pxweb.bfs.admin.ch/DownloadFile.aspx?file={identifier}"
        resp = requests.get(download_url, timeout=30)
        resp.raise_for_status()
        return resp.text
    
    def clean_property_name(self, name: str) -> str:
        """Clean property name for use in URIs"""
        # Convert to camelCase and remove special characters
        name = re.sub(r'[^\w\s]', '', name)
        words = name.split()
        if not words:
            return 'property'
        
        # First word lowercase, rest title case
        clean_name = words[0].lower()
        for word in words[1:]:
            clean_name += word.capitalize()
        
        return clean_name or 'property'
    
    def parse_px_metadata(self, px_content: str) -> Dict:
        """Parse PX file metadata with simplified structure"""
        metadata = {
            "title": {},
            "description": {},
            "stub_dimensions": [],  # Row dimensions
            "heading_dimension": {},  # Column dimension (usually one)
            "values": {}
        }
        
        # Simple regex patterns
        title_pattern = re.compile(r'TITLE(?:\[(\w+)\])?="(.*?)";', re.DOTALL)
        desc_pattern = re.compile(r'DESCRIPTION(?:\[(\w+)\])?="(.*?)";', re.DOTALL)
        stub_pattern = re.compile(r'STUB(?:\[(\w+)\])?="(.*?)";', re.DOTALL)
        heading_pattern = re.compile(r'HEADING(?:\[(\w+)\])?="(.*?)";', re.DOTALL)
        values_pattern = re.compile(r'VALUES\("(.+?)"\)="(.*?)";', re.DOTALL)
        
        lines = px_content.replace('\r\n', '\n').replace('\r', '\n')
        
        # Extract TITLE
        for match in title_pattern.finditer(lines):
            lang = match.group(1) or "de"
            metadata["title"][lang] = match.group(2).strip()
        
        # Extract DESCRIPTION
        for match in desc_pattern.finditer(lines):
            lang = match.group(1) or "de"
            metadata["description"][lang] = match.group(2).strip()
        
        # Extract STUB dimensions
        for match in stub_pattern.finditer(lines):
            lang = match.group(1) or "de"
            dimensions_str = match.group(2)
            # Parse comma-separated values, handling quoted strings
            dimensions = []
            current = ""
            in_quotes = False
            for char in dimensions_str:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == ',' and not in_quotes:
                    if current.strip():
                        dimensions.append(current.strip().strip('"'))
                    current = ""
                else:
                    current += char
            if current.strip():
                dimensions.append(current.strip().strip('"'))
            
            # Store dimensions by language
            for i, dim in enumerate(dimensions):
                while len(metadata["stub_dimensions"]) <= i:
                    metadata["stub_dimensions"].append({})
                if "labels" not in metadata["stub_dimensions"][i]:
                    metadata["stub_dimensions"][i]["labels"] = {}
                metadata["stub_dimensions"][i]["labels"][lang] = dim
        
        # Extract HEADING dimension
        for match in heading_pattern.finditer(lines):
            lang = match.group(1) or "de"
            metadata["heading_dimension"][lang] = match.group(2).strip()
        
        # Extract VALUES (for datatype inference)
        for match in values_pattern.finditer(lines):
            dim_name = match.group(1)
            values_str = match.group(2)
            # Parse values
            values = []
            current = ""
            in_quotes = False
            for char in values_str:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == ',' and not in_quotes:
                    if current.strip():
                        values.append(current.strip().strip('"'))
                    current = ""
                else:
                    current += char
            if current.strip():
                values.append(current.strip().strip('"'))
            
            clean_dim_name = self.clean_property_name(dim_name)
            metadata["values"][clean_dim_name] = values
        
        return metadata
    
    def infer_datatype(self, property_name: str, values: List[str]) -> URIRef:
        """Infer XSD datatype from property name and values"""
        name_lower = property_name.lower()
        
        # Check for date/time indicators
        date_indicators = ['jahr', 'year', 'année', 'anno', 'monat', 'month', 'mois', 'mese', 'datum', 'date']
        if any(indicator in name_lower for indicator in date_indicators):
            if values and len(values[0]) == 4 and values[0].isdigit():
                return self.XSD.gYear  # For years like "2025"
            return self.XSD.date
        
        # Check actual values if available
        if values:
            sample = values[0].strip()
            
            # Try integer
            if sample.isdigit():
                return self.XSD.integer
            
            # Try decimal
            try:
                float(sample)
                return self.XSD.decimal
            except ValueError:
                pass
        
        # Default to string
        return self.XSD.string
    
    def create_clean_shacl_graph(self, dataset_id: str, identifier: str, metadata: Dict):
        """Create clean SHACL graph similar to the example format"""
        
        # Create NodeShape with clean name
        shape_name = f"{identifier}Shape"
        shape_uri = self.i14y[shape_name]
        
        self.g.add((shape_uri, RDF.type, self.SH.NodeShape))
        
        # Add multilingual labels from title
        for lang, title in metadata["title"].items():
            self.g.add((shape_uri, self.RDFS.label, Literal(title, lang=lang)))
        
        # Add multilingual descriptions
        for lang, desc in metadata["description"].items():
            self.g.add((shape_uri, self.DCTERMS.description, Literal(desc, lang=lang)))
        
        # Add timestamps
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        self.g.add((shape_uri, self.DCTERMS.created, Literal(now, datatype=self.XSD.dateTime)))
        self.g.add((shape_uri, self.DCTERMS.modified, Literal(now, datatype=self.XSD.dateTime)))
        
        # Add sh:closed
        self.g.add((shape_uri, self.SH.closed, Literal(True)))
        
        property_order = 0
        
        # Create PropertyShapes for STUB dimensions
        for dim in metadata["stub_dimensions"]:
            if not dim.get("labels"):
                continue
            
            # Use first available label for property name
            first_label = next(iter(dim["labels"].values()))
            prop_name = self.clean_property_name(first_label)
            prop_uri = self.i14y[f"{shape_name}/{prop_name}"]
            
            self.g.add((prop_uri, RDF.type, self.SH.PropertyShape))
            self.g.add((shape_uri, self.SH.property, prop_uri))
            self.g.add((prop_uri, self.SH.path, prop_uri))
            self.g.add((prop_uri, self.SH.order, Literal(property_order)))
            
            # Add multilingual names
            for lang, label in dim["labels"].items():
                self.g.add((prop_uri, self.SH.name, Literal(label, lang=lang)))
            
            # Infer and add datatype
            values = metadata["values"].get(prop_name, [])
            datatype = self.infer_datatype(first_label, values)
            self.g.add((prop_uri, self.SH.datatype, datatype))
            
            # Add basic constraints
            self.g.add((prop_uri, self.SH.minCount, Literal(1)))
            self.g.add((prop_uri, self.SH.maxCount, Literal(1)))
            
            property_order += 1
        
        # Create PropertyShape for HEADING dimension
        if metadata["heading_dimension"]:
            first_label = next(iter(metadata["heading_dimension"].values()))
            prop_name = self.clean_property_name(first_label)
            prop_uri = self.i14y[f"{shape_name}/{prop_name}"]
            
            self.g.add((prop_uri, RDF.type, self.SH.PropertyShape))
            self.g.add((shape_uri, self.SH.property, prop_uri))
            self.g.add((prop_uri, self.SH.path, prop_uri))
            self.g.add((prop_uri, self.SH.order, Literal(property_order)))
            
            # Add multilingual names
            for lang, label in metadata["heading_dimension"].items():
                self.g.add((prop_uri, self.SH.name, Literal(label, lang=lang)))
            
            # Infer and add datatype
            values = metadata["values"].get(prop_name, [])
            datatype = self.infer_datatype(first_label, values)
            self.g.add((prop_uri, self.SH.datatype, datatype))
            
            # Add basic constraints
            self.g.add((prop_uri, self.SH.minCount, Literal(1)))
            self.g.add((prop_uri, self.SH.maxCount, Literal(1)))
    
    def is_px_distribution(self, distribution: dict) -> bool:
        """Check if distribution is a PX file"""
        access_url = self._get_access_url(distribution)
        
        if not access_url or not isinstance(access_url, str):
            return False
        
        clean_url = access_url.split('?')[0].split('#')[0]
        
        px_patterns = [
            r'px-x-\d+_\d+$',
            r'px-x-\d+_\d+\.px$',
            r'/[^/]*px-x-\d+_\d+[^/]*$',
        ]
        
        return any(re.search(pattern, clean_url, re.IGNORECASE) for pattern in px_patterns)
