"""
Format-specific importers - simple classes for different file formats
"""

import re
import os
import csv
import io
from urllib.parse import urlparse
from typing import Dict, List, Optional
import chardet
import urllib


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
            return distribution['accessUrl'].get('uri')  # Extract 'uri' field
        elif isinstance(distribution.get('downloadUrl'), dict):
            return distribution['downloadUrl'].get('uri')  # Extract 'uri' field
        else:
            # Return accessUrl or downloadUrl directly if they are strings
            return distribution.get('accessUrl') or distribution.get('downloadUrl')

    def get_identifier(self, distribution: Dict) -> Optional[str]:
        """Get unique identifier for this file"""
        access_url = self.get_access_url(distribution)
        if not access_url or not isinstance(access_url, str):
            return None  # Ensure access_url is a string

        path = urlparse(access_url).path
        basename = os.path.basename(path)

        if '.' in basename:
            basename = basename.split('.')[0]

        # Ensure the identifier matches the expected pattern
        if re.match(r'px-x-\d+_\d+', basename.lower()):
            return str(basename)  # Ensure it's a string
        return None

    def download_and_parse(self, distribution: Dict, first_n_bytes: int = 1024**2) -> Dict:
        """Download PX file and extract metadata"""
        px_id = self.get_identifier(distribution)
        if not px_id:
            raise Exception("Could not extract PX identifier")

        # Download file
        url = f"https://www.pxweb.bfs.admin.ch/DownloadFile.aspx?file={px_id}"

        not_enough_bytes = True

        while not_enough_bytes:

            # We download only the first megabyte of data
            raw_content = urllib.request.urlopen(url).read(first_n_bytes)

            detected_encoding = chardet.detect(raw_content)['encoding']
            print(f"Detected encoding: {detected_encoding}")  # Debugging: Log detected encoding

            # Decode content using detected encoding
            try:
                px_content = raw_content.decode(detected_encoding or 'utf-8')
            except (UnicodeDecodeError, LookupError):
                # Fallback to UTF-8 with replacement for invalid characters
                px_content = raw_content.decode('utf-8', errors='replace')
                print("Warning: Failed to decode with detected encoding. Falling back to UTF-8 with replacement.")

            not_enough_bytes = "DATA=" not in px_content
            if not_enough_bytes:
                first_n_bytes *= 2
                print(f"DEBUG PXImporter: not enough bytes downloaded, DATA= not detected, first_n_bytes increased to {first_n_bytes}")

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
        if isinstance(media_type, dict):
            # Extract the 'code' field or fallback to an empty string
            media_type_code = media_type.get('code', '').lower()
            if any(indicator in media_type_code for indicator in csv_indicators):
                return True
        elif isinstance(media_type, str):
            if any(indicator in media_type.lower() for indicator in csv_indicators):
                return True
        
        # Check URL extension
        if access_url and access_url.lower().endswith('.csv'):
            return True
        
        return False
    
    def get_access_url(self, distribution: Dict) -> Optional[str]:
        """Get access URL from distribution"""
        if isinstance(distribution.get('accessUrl'), dict):
            return distribution['accessUrl'].get('uri')  # Extract 'uri' field
        elif isinstance(distribution.get('downloadUrl'), dict):
            return distribution['downloadUrl'].get('uri')  # Extract 'uri' field
        else:
            # Return accessUrl or downloadUrl directly if they are strings
            return distribution.get('accessUrl') or distribution.get('downloadUrl')
    
    def get_identifier(self, distribution: Dict) -> Optional[str]:
        """Get unique identifier for this file"""
        access_url = self.get_access_url(distribution)
        if not access_url:
            return None
    
        # Extract the last part of the URL path as the identifier
        identifier = access_url.split('/')[-1].split('?')[0]
        return str(identifier) if identifier else None
    
    def download_and_parse(self, distribution: Dict, first_n_bytes: int = 1024**2) -> Dict:
        """Download CSV file and extract structure"""
        access_url = self.get_access_url(distribution)
        if not access_url:
            raise Exception("No access URL found")
        
        identifier = self.get_identifier(distribution)

        not_enough_bytes = True

        while not_enough_bytes:
        
            # We download only the first megabyte of data
            csv_content = urllib.request.urlopen(access_url).read(first_n_bytes)
            
            # Try different encodings
            content = None
            for encoding in ['utf-8', 'utf-8-sig', 'iso-8859-1']:
                try:
                    content = csv_content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if not content:
                content = csv_content.decode('utf-8', errors='replace')

            not_enough_bytes = "\n" not in content
            if not_enough_bytes:
                first_n_bytes *= 2
                print(f"DEBUG CSVImporter: not enough bytes downloaded, \\n not detected, first_n_bytes increased to {first_n_bytes}")
        
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
    # "px": PXImporter, #TODO Sergiy: decomment
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
