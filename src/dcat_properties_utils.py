from datetime import datetime 
from config import *
from rdflib import URIRef, Literal, Graph
from rdflib.namespace import DCTERMS, FOAF, RDFS, DCAT, RDF
from typing import List, Dict
from rdflib import Namespace
import unicodedata
from mappings import *
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re

VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
SCHEMA = Namespace("http://schema.org/")
PROV = Namespace("http://www.w3.org/ns/prov#")
ADMS = Namespace("http://www.w3.org/ns/adms#")
SPDX = Namespace("http://spdx.org/rdf/terms#")
dcat3 = Namespace("http://www.w3.org/ns/dcat#")

def extract_dataset(graph, dataset_uri):
    """Extracts dataset details from RDF graph."""
    
    distributions = extract_distributions(graph, dataset_uri)
    
    if not has_valid_distributions(distributions):
        print(f"Skipping dataset {dataset_uri} - no valid distributions")
        return None

    dataset = { 
        "identifiers": [get_literal(graph, dataset_uri, DCTERMS.identifier)],
        "title": get_multilingual_literal(graph, dataset_uri, DCTERMS.title),
        "description": get_multilingual_literal(graph, dataset_uri, DCTERMS.description),
        "accessRights": {"code": "PUBLIC"},  
        "issued": get_literal(graph, dataset_uri, DCTERMS.issued, is_date=True),
        "modified": get_literal(graph, dataset_uri, DCTERMS.modified, is_date=True),
        "publisher": DEFAULT_PUBLISHER, 
        "landingPages": get_resource_list(graph, dataset_uri, DCAT.landingPage),
        "keywords": get_multilingual_keywords(graph, dataset_uri, DCAT.keyword),
        "distributions": [dist for dist in distributions if is_valid_distribution(dist)],
        "languages": get_languages(graph, dataset_uri, DCTERMS.language),
        "contactPoints": extract_contact_points(graph, dataset_uri),
        "documentation": get_resource_list(graph, dataset_uri, FOAF.page),
        "images": get_resource_list(graph, dataset_uri, SCHEMA.image),
        "temporalCoverage": get_temporal_coverage(graph, dataset_uri), 
        "frequency": get_frequency(graph, dataset_uri),
        "isReferencedBy": get_is_referenced_by(graph, dataset_uri),
        "relations": get_relations(graph, dataset_uri),
        "spatial": get_spatial(graph, dataset_uri),
        "version": get_literal(graph, dataset_uri, dcat3.version),
        "versionNotes": get_multilingual_literal(graph, dataset_uri, ADMS.versionNotes),
        "conformsTo": get_conforms_to(graph, dataset_uri),
        "themes": get_themes(graph, dataset_uri, DCAT.theme), 
        #"qualifiedRelations": [{"hadRole":{"code":"original"}, "relation":{"uri":get_literal(graph, dataset_uri, DCTERMS.identifier)}}]
        
    }

    if not dataset["description"]:
        print("no description found")
        return None

    return dataset


def extract_distributions(graph, dataset_uri):
    """Extracts distributions for a dataset."""
    distributions = []
    for distribution_uri in graph.objects(dataset_uri, DCAT.distribution):
        title = get_multilingual_literal(graph, distribution_uri, DCTERMS.title)
        description = get_multilingual_literal(graph, distribution_uri, DCTERMS.description)
        if not title: 
            title = {'de': 'Datenexport'}
        if not description:  
            description = {'de': 'Export der Daten'}

        media_type_uri = get_single_resource(graph, distribution_uri, DCAT.mediaType)
    
        format_uri = get_single_resource(graph, distribution_uri, DCTERMS.format)
        format_code = None

        if format_uri:
            format_uri_str = str(format_uri)
            if format_uri_str in FORMAT_TYPE_MAPPING:
                format_code = FORMAT_TYPE_MAPPING[format_uri_str]
            else:
                format_code = format_uri_str.split("/")[-1].upper()

        download_url = get_single_resource(graph, distribution_uri, DCAT.downloadURL)
        access_url = get_single_resource(graph, distribution_uri, DCAT.accessURL)
        common_url = access_url if access_url else download_url
        download_title = get_multilingual_literal(graph, distribution_uri, RDFS.label)
        availability_uri = get_single_resource(graph, distribution_uri, URIRef("http://data.europa.eu/r5r/availability"))
        license_uri = get_single_resource(graph, distribution_uri, DCTERMS.license)
        license_code = license_uri.split("/")[-1] if license_uri else None
        checksum_algorithm = get_literal(graph, distribution_uri, SPDX.checksumAlgorithm)
        checksum_value = get_literal(graph, distribution_uri, SPDX.checksumValue)
        packaging_format = get_literal(graph, distribution_uri, DCAT.packageFormat)

        distribution = {
            "title": title, 
            "description": description,  
            "format": {"code": format_code} if format_code and format_code in VALID_FORMAT_CODES else None,  
            "downloadUrl": {
                "label": download_title,  
                "uri": download_url if download_url else common_url
            } if common_url else None,
           "mediaType": {"code": get_media_type(media_type_uri)} if media_type_uri and get_media_type(media_type_uri) else None,
            "accessUrl": {
                "label": download_title,  
                "uri": common_url 
            } if common_url else None,
            "license": {"code": license_code} if license_code else None,  
            "availability": {"code": get_availability_code(availability_uri)} if get_availability_code(availability_uri) else None,  
            "issued": get_literal(graph, distribution_uri, DCTERMS.issued, is_date=True),
            "modified": get_literal(graph, distribution_uri, DCTERMS.modified, is_date=True),
            "rights": get_literal(graph, distribution_uri, DCTERMS.rights),
            "accessServices": get_access_services(graph, distribution_uri),
            "byteSize": get_literal(graph, distribution_uri, DCAT.byteSize),
            "checksum": {
                "algorithm": {"code": checksum_algorithm} if checksum_algorithm else None,
                "checksumValue": checksum_value
            } if checksum_algorithm or checksum_value else None,
            "conformsTo": get_conforms_to(graph, distribution_uri),
            "coverage": get_coverage(graph, distribution_uri),
            "documentation": get_resource_list(graph, distribution_uri, FOAF.page),
            "identifier": get_literal(graph, distribution_uri, DCTERMS.identifier),
            "images": get_resource_list(graph, distribution_uri, SCHEMA.image),
            "languages": get_languages(graph, distribution_uri, DCTERMS.language),
            "packagingFormat": {"code": packaging_format} if packaging_format else None,
            "spatialResolution": get_literal(graph, distribution_uri, DCAT.spatialResolutionInMeters), 
            "temporalResolution": get_literal(graph, distribution_uri, DCAT.temporalResolution)
        }
        distributions.append(distribution)
    return distributions


def is_valid_distribution(distribution):
    """Check if a distribution is valid (not PDF)."""
    if not distribution.get('mediaType'):
        return False
    
    # Check media type
    media_code = distribution['mediaType'].get('code', '').lower()
    excluded_media_types = ['application/pdf']
    
    # Check format if available
    format_code = None
    if distribution.get('format') and distribution['format'].get('code'):
        format_code = distribution['format']['code'].upper()
    
    # Exclusion list for format codes
    excluded_format_codes = ['PDF']
    
    # Distribution is invalid if:
    # 1. Media type is in excluded list OR
    # 2. Format code is in excluded list
    if (media_code in excluded_media_types) or (format_code in excluded_format_codes):
        return False
    
    return True


def has_valid_distributions(distributions):
    """Check if a dataset has at least one valid distribution."""
    if not distributions:
        return False
    return any(is_valid_distribution(dist) for dist in distributions)
    

def remove_html_tags(text):
    """Remove HTML tags using BeautifulSoup."""
    return BeautifulSoup(text, "html.parser").get_text()


def get_languages(graph, subject, predicate):
    """Retrieves a list of i14y codes for themes."""
    languages = []
    for lang_uri in graph.objects(subject, predicate):
        lang_uri = str(lang_uri)
        for code, uris in LANGUAGES_MAPPING.items():  
            if lang_uri in uris: 
                languages.append({"code": code})
                break  
    return languages

def get_multilingual_literal(graph, subject, predicate):
    """Retrieves multilingual literals from RDF graph."""
    values = {lang: "" for lang in ["de", "en", "fr", "it", "rm"]}  
    for obj in graph.objects(subject, predicate):
        if isinstance(obj, Literal) and obj.language in values:
            cleaned_text = remove_html_tags(str(obj)) 
            values[obj.language] = cleaned_text

    return {lang: value for lang, value in values.items() if value}

def get_literal(graph, subject, predicate, is_date=False):
    """
    Retrieves a single value from the RDF graph.
    - If the value is a URI or literal, it returns it as a string.
    - If the value is a date and `is_date=True`, it formats it as ISO 8601.
    """
    value = graph.value(subject, predicate)
    if not value:
        return None 

    value_str = str(value)  

    if is_date:
        try:
          
            if len(value_str) == 10: 
                dt = datetime.strptime(value_str, "%Y-%m-%d")
                return dt.strftime("%Y-%m-%dT00:00:00Z")
            else: 
                dt = datetime.fromisoformat(value_str.replace("Z", "+00:00"))
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return value_str 

    return value_str  


def get_single_resource(graph, subject, predicate):
    """Retrieves a single resource (URI) for a given predicate."""
    uri = graph.value(subject, predicate)
    return str(uri) if uri else None  

def get_resource_list(graph, subject, predicate):
    """Retrieves a list of resources (URIs) for a given predicate."""
    return [{"uri": str(uri)} for uri in graph.objects(subject, predicate)]

def get_resource_codes(graph, subject, predicate):
    """Retrieves a list of resource codes."""
    return [{"code": str(obj)} for obj in graph.objects(subject, predicate)]

def normalize_text(text):
    """Normalizes text by removing special characters and converting to lowercase."""
    normalized_text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    return normalized_text.lower().strip()


def get_multilingual_keywords(graph: Graph, subject: URIRef, predicate: URIRef) -> List[Dict]:
    """Retrieves only keywords with explicit language tags."""
    return [
        {str(lang): str(keyword_obj)}
        for keyword_obj in graph.objects(subject, predicate)
        if keyword_obj is not None and (lang := getattr(keyword_obj, 'language', None))
    ]


def get_media_type(media_type_uri: str) -> str:
    """Returns the media type code if it's a valid URI or direct code."""
    if not media_type_uri:
        return None  
    if media_type_uri in MEDIA_TYPE_MAPPING.values():
        return media_type_uri

    return MEDIA_TYPE_MAPPING.get(str(media_type_uri))

def get_access_services(graph, subject):
    """Retrieves accessServices from RDF graph."""
    return [{"id": str(obj)} for obj in graph.objects(subject, DCAT.accessService)]


def get_conforms_to(graph, subject):
    """Retrieves conformsTo from RDF graph."""
    return [{
        "label": get_multilingual_literal(graph, obj, DCTERMS.title),
        "uri": str(obj)
    } for obj in graph.objects(subject, DCTERMS.conformsTo)]


def get_coverage(graph, subject):
    """Retrieves coverage from RDF graph."""
    coverage = []
    for obj in graph.objects(subject, DCTERMS.coverage):
        start = get_literal(graph, obj, DCTERMS.start)
        end = get_literal(graph, obj, DCTERMS.end)
        if start or end:
            coverage.append({
                "start": start,
                "end": end
            })
    return coverage


def get_spatial(graph, dataset_uri):
    """
    Retrieves spatial value(s) and returns them as a list of strings.
    Handles both URI resources (like "Kanton Basel-Landschaft") and literals.
    """
    spatial_values = []
    for spatial in graph.objects(dataset_uri, DCTERMS.spatial):
     
        if isinstance(spatial, URIRef):
            spatial_str = str(spatial).split("/")[-1]  
        else:
            spatial_str = str(spatial)
        spatial_values.append(spatial_str)
    
    return spatial_values if spatial_values else []


def get_frequency(graph, subject):
    """Retrieves frequency from RDF graph."""
    frequency_uri = get_single_resource(graph, subject, DCTERMS.accrualPeriodicity)
    return {"code": frequency_uri.split("/")[-1]} if frequency_uri else None


def get_themes(graph, subject, predicate):
    """
    Retrieves a list of unique i14y codes for themes.
    Handles both literal values (e.g., "101") and URI values (e.g., "http://publications.europa.eu/resource/authority/data-theme/ECON").
    Ensures that the collection does not contain repeated codes.
    """
    unique_codes = set()  
    themes = []

    for theme in graph.objects(subject, predicate):
        theme_str = str(theme) 
        theme_code = None

        if isinstance(theme, Literal):
            theme_code = theme_str 

        elif isinstance(theme, URIRef):
      
            for code, uris in THEME_MAPPING.items():
                if theme_str in uris:
                    theme_code = code
                    break
        if theme_code and theme_code not in unique_codes:
            unique_codes.add(theme_code) 
            themes.append({"code": theme_code})
    if not themes:
         themes.append({"code": "125"})
    return themes


def get_availability_code(availability_uri):
    """Maps an availability URI to its corresponding code using the vocabulary."""
    if not availability_uri:
        return None
    for code, uris in VOCAB_EU_PLANNED_AVAILABILITY.items():
        if availability_uri in uris:
            return code
    return None


def get_temporal_coverage(graph, subject):
    """Retrieves properly structured temporal coverage data from RDF graph."""
    temporal_coverage = []

    for obj in graph.objects(subject, DCTERMS.temporal):
        if (obj, RDF.type, DCTERMS.PeriodOfTime) in graph:
            start = get_literal(graph, obj, DCAT.startDate, is_date=True)
            end = get_literal(graph, obj, DCAT.endDate, is_date=True)
            if start or end:
                temporal_coverage.append({
                    "start": start if start else None,
                    "end": end if end else None
                })

    return temporal_coverage



def get_is_referenced_by(graph, subject):
    """Retrieves isReferencedBy from RDF graph."""
    return [{
        "uri": str(obj)
    } for obj in graph.objects(subject, DCTERMS.isReferencedBy)]


def get_qualified_relations(graph, subject):
    """Retrieves qualifiedRelations from RDF graph."""
    relations = []
    for obj in graph.objects(subject, PROV.qualifiedRelation):
        had_role = get_single_resource(graph, obj, PROV.hadRole)
        relation = get_single_resource(graph, obj, DCTERMS.relation)
        if had_role and relation:
            relations.append({
                "hadRole": {"code": had_role.split("/")[-1]},
                "relation": {
                    "label": get_multilingual_literal(graph, relation, RDFS.label),
                    "uri": str(relation)
                }
            })
    return relations


def get_qualified_attributions(graph, subject):
    """Retrieves qualifiedAttributions from RDF graph."""
    attributions = []
    for obj in graph.objects(subject, PROV.qualifiedAttribution):
        agent = get_single_resource(graph, obj, PROV.agent)
        had_role = get_single_resource(graph, obj, PROV.hadRole)
        if agent and had_role:
            attributions.append({
                "agent": {"identifier": agent},
                "hadRole": {"code": had_role.split("/")[-1]}
            })
    return attributions



def get_relations(graph, subject):
    """Retrieves relations from RDF graph, handling malformed URIs with semicolons."""
    relations = []
    for obj in graph.objects(subject, DCTERMS.relation):
        original_uri = str(obj)
        
        potential_uris = re.split(r';\s+', original_uri.strip('; \t\n\r'))
        
        for uri in potential_uris:
            uri = uri.strip()
            if not uri:
                continue
                
            if is_valid_uri(uri):
                relations.append({
                    "label": get_multilingual_literal(graph, obj, RDFS.label),
                    "uri": uri
                })
            else:
                print(f"Skipping invalid relation URI: {uri}")
    
    return relations

def is_valid_uri(uri):
    """Check if the string looks like a valid URI"""
    try:
        result = urlparse(uri)
        return all([result.scheme, result.netloc])
    except:
        return False

def get_conforms_to(graph, subject):
    """Retrieves conformsTo from RDF graph."""
    return [{
        "label": get_multilingual_literal(graph, obj, RDFS.label),
        "uri": str(obj)
    } for obj in graph.objects(subject, DCTERMS.conformsTo)]


def extract_contact_points(graph, dataset_uri):
    """Extracts contact points from RDF, including name, email, etc."""
    contact_points = []
    
    for contact_uri in graph.objects(dataset_uri, DCAT.contactPoint):
        fn = str(graph.value(contact_uri, VCARD.fn))  
        if not fn:
            fn = get_multilingual_literal(graph, contact_uri, VCARD.fn)
   
        email = str(graph.value(contact_uri, VCARD.hasEmail))
        if email and email.startswith("mailto:"):
            email = email[7:]  
 
        address = get_multilingual_literal(graph, contact_uri, VCARD.hasAddress)
        telephone = get_literal(graph, contact_uri, VCARD.hasTelephone)
        note = get_multilingual_literal(graph, contact_uri, VCARD.note)
        if fn or email or address or telephone or note:
            contact_points.append({
                "fn": {"de": fn} if fn else {"de": "", "en": "", "fr": "", "it": "", "rm": ""},
                "hasAddress": address if address else {"de": "", "en": "", "fr": "", "it": "", "rm": ""},
                "hasEmail": email,
                "hasTelephone": telephone,
                "kind": "Organization",
                "note": note if note else {"de": "", "en": "", "fr": "", "it": "", "rm": ""}
            })

    return contact_points
