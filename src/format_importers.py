"""
Format-specific importers - simple classes for different file formats
"""

import re
import requests
import os
import csv
import io
from urllib.parse import urlparse
from typing import Dict, List, Optional


class PXImporter:
    """Handles PX file operations"""
    
    def can_process(self, distribution: Dict) -> bool:
        """Check if this distribution is a PX file"""
        access_url = self.get_access_url(distribution)
        if not access_url:
            return False
        
        clean_url = access_url.split('?')[0].split('#')[0]
        return bool(re.search(r'px-x-\d+_\d+', clean_url, re.IGNORECASE))
    
    def get_access_url(self, distribution: Dict) -> Optional[str]:
        """Get access URL from distribution"""
        if isinstance(distribution.get('accessUrl'), dict):
            return distribution['accessUrl'].get('uri')
        elif isinstance(distribution.get('downloadUrl'), dict):
            return distribution['downloadUrl'].get('uri')
        else:
            return distribution.get('accessUrl') or distribution.get('downloadUrl')
    
    def get_identifier(self, distribution: Dict) -> Optional[str]:
        """Get unique identifier for this file"""
        access_url = self.get_access_url(distribution)
        if not access_url:
            return None
            
        path = urlparse(access_url).path
        basename = os.path.basename(path)
        
        if '.' in basename:
            basename = basename.split('.')[0]
        
        if re.match(r'px-x-\d+_\d+', basename.lower()):
            return basename
        return None
    
    def download_and_parse(self, distribution: Dict) -> Dict:
        """Download file and extract metadata"""
        px_id = self.get_identifier(distribution)
        if not px_id:
            raise Exception("Could not extract PX identifier")
        
        # Download file
        url = f"https://www.pxweb.bfs.admin.ch/DownloadFile.aspx?file={px_id}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Explicitly set encoding (e.g., UTF-8)
        response.encoding = 'utf-8'
        px_content = response.text
        
        # Parse metadata
        return self.parse_px_content(px_content, px_id)
    
    def parse_px_content(self, px_content: str, px_id: str) -> Dict:
        """Parse PX file content"""
        data = {
            "identifier": px_id,
            "title": {},
            "description": {},
            "properties": []
        }
        
        lines = px_content.replace('\r\n', '\n').replace('\r', '\n')
        
        # Extract TITLE
        for match in re.finditer(r'TITLE(?:\[(\w+)\])?="(.*?)";', lines, re.DOTALL):
            lang = match.group(1) or "de"
            data["title"][lang] = match.group(2).strip()
        
        # Extract DESCRIPTION
        for match in re.finditer(r'DESCRIPTION(?:\[(\w+)\])?="(.*?)";', lines, re.DOTALL):
            lang = match.group(1) or "de"
            data["description"][lang] = match.group(2).strip()
        
        # Extract STUB dimensions
        stub_dimensions = []
        for match in re.finditer(r'STUB(?:\[(\w+)\])?="(.*?)";', lines, re.DOTALL):
            lang = match.group(1) or "de"
            dimensions_str = match.group(2)
            
            dimensions = []
            parts = dimensions_str.split('","')
            for part in parts:
                clean_part = part.strip().strip('"')
                if clean_part:
                    dimensions.append(clean_part)
            
            for i, dim in enumerate(dimensions):
                while len(stub_dimensions) <= i:
                    stub_dimensions.append({})
                if lang not in stub_dimensions[i]:
                    stub_dimensions[i][lang] = dim
        
        # Extract HEADING dimension
        heading_dimension = {}
        for match in re.finditer(r'HEADING(?:\[(\w+)\])?="(.*?)";', lines, re.DOTALL):
            lang = match.group(1) or "de"
            heading_dimension[lang] = match.group(2).strip()
        
        # Convert to properties format
        for dim_data in stub_dimensions:
            if dim_data:
                first_name = next(iter(dim_data.values()))
                prop_name = self.clean_property_name(first_name)
                data["properties"].append({
                    "name": prop_name,
                    "labels": dim_data,
                    "datatype": "string"
                })
        
        if heading_dimension:
            first_name = next(iter(heading_dimension.values()))
            prop_name = self.clean_property_name(first_name)
            # Check if it's a year
            is_year = any('jahr' in name.lower() or 'year' in name.lower() 
                         for name in heading_dimension.values())
            data["properties"].append({
                "name": prop_name,
                "labels": heading_dimension,
                "datatype": "gYear" if is_year else "string"
            })
        
        return data
    
    def clean_property_name(self, name: str) -> str:
        """Convert to camelCase property name"""
        name = re.sub(r'[^\w\s]', '', name)
        words = name.split()
        if not words:
            return 'property'
        
        result = words[0].lower()
        for word in words[1:]:
            result += word.capitalize()
        
        return result or 'property'


class CSVImporter:
    """Handles CSV file operations"""
    
    def can_process(self, distribution: Dict) -> bool:
        """Check if this distribution is a CSV file"""
        format_info = distribution.get('format', {})
        media_type = distribution.get('mediaType', '')
        access_url = self.get_access_url(distribution)
        
        csv_indicators = ['csv', 'text/csv', 'application/csv']
        
        # Check format
        if isinstance(format_info, dict):
            format_name = format_info.get('name', '').lower()
            if any(indicator in format_name for indicator in csv_indicators):
                return True
        
        # Check media type
        if any(indicator in media_type.lower() for indicator in csv_indicators):
            return True
        
        # Check URL extension
        if access_url and access_url.lower().endswith('.csv'):
            return True
        
        return False
    
    def get_access_url(self, distribution: Dict) -> Optional[str]:
        """Get access URL from distribution"""
        if isinstance(distribution.get('accessUrl'), dict):
            return distribution['accessUrl'].get('uri')
        elif isinstance(distribution.get('downloadUrl'), dict):
            return distribution['downloadUrl'].get('uri')
        else:
            return distribution.get('accessUrl') or distribution.get('downloadUrl')
    
    def get_identifier(self, distribution: Dict) -> Optional[str]:
        """Get unique identifier for this file"""
        access_url = self.get_access_url(distribution)
        if access_url:
            return access_url.split('/')[-1].split('?')[0]
        return None
    
    def download_and_parse(self, distribution: Dict) -> Dict:
        """Download CSV file and extract structure"""
        access_url = self.get_access_url(distribution)
        if not access_url:
            raise Exception("No access URL found")
        
        identifier = self.get_identifier(distribution)
        
        # Download file
        response = requests.get(access_url, timeout=30)
        response.raise_for_status()
        
        # Try different encodings
        content = None
        for encoding in ['utf-8', 'utf-8-sig', 'iso-8859-1']:
            try:
                content = response.content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        
        if not content:
            content = response.content.decode('utf-8', errors='replace')
        
        return self.parse_csv_content(content, identifier)
    
    def parse_csv_content(self, csv_content: str, identifier: str) -> Dict:
        """Parse CSV content and extract structure"""
        # Try different delimiters
        delimiters = [',', ';', '\t']
        best_delimiter = ','
        max_columns = 0
        
        for delimiter in delimiters:
            try:
                sample_reader = csv.reader(io.StringIO(csv_content[:1000]), delimiter=delimiter)
                first_row = next(sample_reader, [])
                if len(first_row) > max_columns:
                    max_columns = len(first_row)
                    best_delimiter = delimiter
            except:
                continue
        
        # Parse with best delimiter
        reader = csv.reader(io.StringIO(csv_content), delimiter=best_delimiter)
        
        try:
            headers = next(reader)
            rows = list(reader)
        except StopIteration:
            raise Exception("Empty CSV file")
        
        # Create structure
        data = {
            "identifier": identifier,
            "title": {"en": f"CSV Structure for {identifier}"},
            "description": {"en": f"Automatically generated structure for CSV file with {len(headers)} columns"},
            "properties": []
        }
        
        # Analyze each column
        for i, header in enumerate(headers):
            column_values = [row[i] if i < len(row) else '' for row in rows[:50]]
            prop_name = self.clean_property_name(header)
            datatype = self.infer_datatype(column_values)
            
            data["properties"].append({
                "name": prop_name,
                "labels": {"en": header},
                "datatype": datatype
            })
        
        return data
    
    def clean_property_name(self, name: str) -> str:
        """Convert to camelCase property name"""
        name = re.sub(r'[^\w\s]', '', str(name))
        words = name.split()
        if not words:
            return 'column'
        
        result = words[0].lower()
        for word in words[1:]:
            result += word.capitalize()
        
        return result or 'column'
    
    def infer_datatype(self, values: List[str]) -> str:
        """Infer datatype from values"""
        non_empty = [v.strip() for v in values if v.strip()]
        if not non_empty:
            return "string"
        
        # Try integer
        try:
            for v in non_empty:
                int(v)
            return "integer"
        except ValueError:
            pass
        
        # Try decimal
        try:
            for v in non_empty:
                float(v)
            return "decimal"
        except ValueError:
            pass
        
        return "string"


# Registry of available importers
IMPORTERS = {
    "px": PXImporter,
    "csv": CSVImporter,
    # Add more importers here:
    # "json": JSONImporter,
    # "xml": XMLImporter,
}


def get_suitable_importer(distribution: Dict):
    """Find the right importer for a distribution"""
    for name, importer_class in IMPORTERS.items():
        importer = importer_class()
        if importer.can_process(distribution):
            return importer, name
    return None, None