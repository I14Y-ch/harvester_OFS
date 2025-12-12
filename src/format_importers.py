"""
Format-specific importers - simple classes for different file formats
"""

import datetime
import re
import os
import csv
import io
from urllib.parse import urlparse
from typing import Dict, List, Optional
import chardet
import urllib


class FormatImporter:
    """Common functions for all importers"""

    YEAR_KEYWORDS = {"jahr", "year", "année", "annee", "anno"}
    DATE_FORMATS = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d.%m.%Y",
        "%d.%m.%y",
    ]
    BOOLEAN_TRUE = {"1", "oui", "ja", "si"}
    BOOLEAN_FALSE = {"0", "non", "nein", "no"}

    def get_access_url(self, distribution: Dict) -> Optional[str]:
        """Get access URL from distribution"""
        if isinstance(distribution.get("accessUrl"), dict):
            return distribution["accessUrl"].get("uri")  # Extract 'uri' field
        elif isinstance(distribution.get("downloadUrl"), dict):
            return distribution["downloadUrl"].get("uri")  # Extract 'uri' field
        else:
            # Return accessUrl or downloadUrl directly if they are strings
            return distribution.get("accessUrl") or distribution.get("downloadUrl")

    def decode_content(self, raw_content: bytes):
        detected_encoding = chardet.detect(raw_content)["encoding"]
        print(f"Detected encoding: {detected_encoding}")  # Debugging: Log detected encoding

        # Decode content using detected encoding
        try:
            content = raw_content.decode(detected_encoding or "utf-8")
        except (UnicodeDecodeError, LookupError):
            # Fallback to UTF-8 with replacement for invalid characters
            content = raw_content.decode("utf-8", errors="replace")
            print("Warning: Failed to decode with detected encoding. Falling back to UTF-8 with replacement.")

        return content


class PXImporter(FormatImporter):
    """Handles PX file operations"""

    def can_process(self, distribution: Dict) -> bool:
        """Check if this distribution is a PX file"""
        access_url = self.get_access_url(distribution)
        if not access_url:
            return False

        clean_url = access_url.split("?")[0].split("#")[0]
        return bool(re.search(r"px-x-\d+_\d+", clean_url, re.IGNORECASE))

    def get_identifier(self, distribution: Dict) -> Optional[str]:
        """Get unique identifier for this file"""
        access_url = self.get_access_url(distribution)
        if not access_url or not isinstance(access_url, str):
            return None  # Ensure access_url is a string

        path = urlparse(access_url).path
        basename = os.path.basename(path)

        if "." in basename:
            basename = basename.split(".")[0]

        # Ensure the identifier matches the expected pattern
        if re.match(r"px-x-\d+_\d+", basename.lower()):
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

            px_content = self.decode_content(raw_content)

            px_content = px_content.replace("\r\n", "\n").replace("\r", "\n")

            match = re.search(r"DATA\s*=\s*(.*)", px_content, re.DOTALL)
            # We check if DATA= is present
            not_enough_bytes = "DATA=" not in px_content

            if not_enough_bytes:
                first_n_bytes *= 2
                print(
                    f"DEBUG PXImporter: not enough bytes downloaded, DATA= not detected, first_n_bytes increased to {first_n_bytes}"
                )

        # Parse metadata
        return self.parse_px_content(px_content, px_id)

    def parse_px_content(self, px_content: str, px_id: str) -> Dict:
        """Parse PX file content"""
        data = {"identifier": px_id, "title": {}, "description": {}, "properties": []}

        lines = px_content

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

        # Extract HEADING dimensions
        heading_dimensions = []
        for match in re.finditer(r'HEADING(?:\[(\w+)\])?="(.*?)";', lines, re.DOTALL):
            lang = match.group(1) or "de"
            dimensions_str = match.group(2)

            parts = dimensions_str.split('","')
            dimensions = []
            for part in parts:
                clean_part = part.strip().strip('"')
                if clean_part:
                    dimensions.append(clean_part)

            for i, dim in enumerate(dimensions):
                while len(heading_dimensions) <= i:
                    heading_dimensions.append({})
                heading_dimensions[i][lang] = dim


        # Convert to properties format
        for dim_data in stub_dimensions:
            if dim_data:
                first_name = next(iter(dim_data.values()))
                prop_name = self.clean_property_name(first_name)
                is_year = any(keyword in first_name.lower() for keyword in self.YEAR_KEYWORDS)
                data["properties"].append({"name": prop_name, "labels": dim_data, "datatype": "gYear" if is_year else "string"})

        for dim_data in heading_dimensions:
            if dim_data:
                first_name = next(iter(dim_data.values()))
                prop_name = self.clean_property_name(first_name)
                is_year = any(keyword in first_name.lower() for keyword in self.YEAR_KEYWORDS)
                data["properties"].append(
                    {"name": prop_name, "labels": dim_data, "datatype": "gYear" if is_year else "string"}
                )

        return data

    def clean_property_name(self, name: str) -> str:
        """Convert to camelCase property name"""
        name = re.sub(r"[^\w\s]", "", name)
        words = name.split()
        if not words:
            return "property"

        result = words[0].lower()
        for word in words[1:]:
            result += word.capitalize()

        return result or "property"


class CSVImporter(FormatImporter):
    """Handles CSV file operations"""

    def can_process(self, distribution: Dict) -> bool:
        """Check if this distribution is a CSV file"""
        format_info = distribution.get("format", {})
        media_type = distribution.get("mediaType", "")
        access_url = self.get_access_url(distribution)

        csv_indicators = ["csv", "text/csv", "application/csv"]

        # Check format
        if isinstance(format_info, dict):
            format_name = format_info.get("name", "")
            if isinstance(format_name, str):
                format_name = format_name.lower()
                if any(indicator in format_name for indicator in csv_indicators):
                    return True
            # If format_name is a dict, we use format_info['code']
            elif isinstance(format_name, dict):
                if "code" in format_info.keys():
                    format_code = format_info["code"]
                    if isinstance(format_code, str):
                        format_code = format_code.lower()
                        if any(indicator in format_code for indicator in csv_indicators):
                            return True

        # Check media type
        if isinstance(media_type, dict):
            # Extract the 'code' field or fallback to an empty string
            media_type_code = media_type.get("code", "").lower()
            if any(indicator in media_type_code for indicator in csv_indicators):
                return True
        elif isinstance(media_type, str):
            if any(indicator in media_type.lower() for indicator in csv_indicators):
                return True

        # Check URL extension
        if access_url and access_url.lower().endswith(".csv"):
            return True

        return False

    def get_identifier(self, distribution: Dict) -> Optional[str]:
        """Get unique identifier for this file"""
        access_url = self.get_access_url(distribution)
        if not access_url:
            return None

        # Extract the last part of the URL path as the identifier
        identifier = access_url.split("/")[-1].split("?")[0]
        # The access_url for csvs are often https://dam-api.bfs.admin.ch/hub/api/dam/assets/36158430/master
        # So we need the part before "master" if the extracted identifier is "master"
        if identifier and identifier == "master":
            identifier = access_url.split("/")[-2].split("?")[0]
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
            raw_content = urllib.request.urlopen(access_url).read(first_n_bytes)

            content = self.decode_content(raw_content)

            content = content.replace("\r\n", "\n").replace("\r", "\n")

            not_enough_bytes = len([line for line in content.split("\n") if line.strip() != ""]) < 2
            if not_enough_bytes:
                first_n_bytes *= 2
                print(
                    f"DEBUG CSVImporter: not enough bytes downloaded, \\n not detected, first_n_bytes increased to {first_n_bytes}"
                )

        return self.parse_csv_content(content, identifier)

    def parse_csv_content(self, csv_content: str, identifier: str) -> Dict:
        """Parse CSV content and extract structure"""
        # Try different delimiters
        delimiters = [",", ";", "\t"]
        best_delimiter = ","
        max_columns = 0

        for delimiter in delimiters:
            try:
                sample_reader = csv.reader(io.StringIO(csv_content), delimiter=delimiter)
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
            "properties": [],
        }

        # Analyze each column
        for i, header in enumerate(headers):
            column_values = [row[i] if i < len(row) else "" for row in rows[:50]]
            prop_name = self.clean_property_name(header)
            is_year = any(keyword in prop_name.lower() for keyword in self.YEAR_KEYWORDS)
            datatype = "gYear" if is_year else self.infer_datatype(column_values)

            data["properties"].append({"name": prop_name, "labels": {"en": header}, "datatype": datatype})

        return data

    def clean_property_name(self, name: str) -> str:
        """Convert to camelCase property name"""
        name = re.sub(r"[^\w\s]", "", str(name))
        words = name.split()
        if not words:
            return "column"

        result = words[0].lower()
        for word in words[1:]:
            result += word.capitalize()

        return result or "column"

    def is_date(self, value: str) -> bool:
        for fmt in self.DATE_FORMATS:
            try:
                datetime.datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False

    def infer_datatype(self, values: List[str]) -> str:
        """Infer datatype from values"""
        non_empty = [v.strip().lower() for v in values if v.strip()]
        if not non_empty:
            return "string"

        if all(v in self.BOOLEAN_TRUE or v in self.BOOLEAN_FALSE for v in non_empty):
            return "boolean"

        try:
            float_values = [float(v.replace(',', '.')) for v in non_empty]
            if all(f.is_integer() for f in float_values):
                return "integer"
            return "decimal"
        except ValueError:
            pass

        if all(self.is_date(v) for v in non_empty):
            return "date"

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
