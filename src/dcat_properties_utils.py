from datetime import datetime 
from config import *
from rdflib import URIRef, Literal
from rdflib.namespace import DCTERMS, FOAF, RDFS, DCAT, RDF
from rdflib import Namespace
import unicodedata
from mappings import *
from bs4 import BeautifulSoup

VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
SCHEMA = Namespace("http://schema.org/")
PROV = Namespace("http://www.w3.org/ns/prov#")



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


def get_multilingual_keywords(graph, subject, predicate):
    """Retrieves only keywords with explicit language tags"""
    keywords = []
    
    for keyword_obj in graph.objects(subject, predicate):
        lang = getattr(keyword_obj, 'language', None)
        if lang:  
            keywords.append({lang: str(keyword_obj)})
    
    return keywords


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
    """Retrieves relations from RDF graph."""
    return [{
        "label": get_multilingual_literal(graph, obj, RDFS.label),
        "uri": str(obj)
    } for obj in graph.objects(subject, DCTERMS.relation)]



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
