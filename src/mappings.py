# i14y theme code mapping.
#
# Important: these are i14y DV_DCAT_DATASET_THEME codes, not exact DCAT-CH
# statistical themes. Some codes are close to DCAT-CH themes, but not identical
# (for example 110 = Infrastructure, while 116 = Mobility).
THEME_MAPPING = {
    "101": ("http://dcat-ap.ch/vocabulary/themes/work", "http://publications.europa.eu/resource/authority/data-theme/ECON"),
    "102": ("http://dcat-ap.ch/vocabulary/themes/construction", "http://publications.europa.eu/resource/authority/data-theme/SOCI"),
    "103": ("http://dcat-ap.ch/vocabulary/themes/education", "http://publications.europa.eu/resource/authority/data-theme/EDUC"),
    "104": (),  # External relations: no stable EU data-theme candidate in the current input mapping.
    "105": ("http://dcat-ap.ch/vocabulary/themes/crime", "http://publications.europa.eu/resource/authority/data-theme/JUST"),
    "106": ("http://publications.europa.eu/resource/authority/data-theme/SOCI",),
    "107": ("http://dcat-ap.ch/vocabulary/themes/politics", "http://publications.europa.eu/resource/authority/data-theme/GOVE"),
    "108": ("http://dcat-ap.ch/vocabulary/themes/culture", "http://publications.europa.eu/resource/authority/data-theme/EDUC"),
    "109": ("http://dcat-ap.ch/vocabulary/themes/agriculture", "http://publications.europa.eu/resource/authority/data-theme/AGRI"),
    "110": (),  # Infrastructure. Do not use for TRAN; mobility is 116.
    "111": ("http://dcat-ap.ch/vocabulary/themes/public-order", "http://publications.europa.eu/resource/authority/data-theme/GOVE"),
    "112": ("http://publications.europa.eu/resource/authority/data-theme/ECON",),  # Taxes.
    "113": ("http://dcat-ap.ch/vocabulary/themes/territory", "http://publications.europa.eu/resource/authority/data-theme/ENVI"),
    "114": ("http://dcat-ap.ch/vocabulary/themes/health", "http://publications.europa.eu/resource/authority/data-theme/HEAL"),
    "115": (
        "http://dcat-ap.ch/vocabulary/themes/national-economy",
        "http://dcat-ap.ch/vocabulary/themes/prices",
        "http://dcat-ap.ch/vocabulary/themes/finances",
        "http://dcat-ap.ch/vocabulary/themes/trade",
        "http://dcat-ap.ch/vocabulary/themes/tourism",
        "http://dcat-ap.ch/vocabulary/themes/industry",
        "http://publications.europa.eu/resource/authority/data-theme/ECON",
    ),
    "116": ("http://dcat-ap.ch/vocabulary/themes/mobility", "http://publications.europa.eu/resource/authority/data-theme/TRAN"),
    "117": ("http://dcat-ap.ch/vocabulary/themes/population", "http://publications.europa.eu/resource/authority/data-theme/SOCI"),
    "118": ("http://dcat-ap.ch/vocabulary/themes/industry", "http://publications.europa.eu/resource/authority/data-theme/ECON"),
    "119": ("http://dcat-ap.ch/vocabulary/themes/administration", "http://publications.europa.eu/resource/authority/data-theme/GOVE"),
    "120": ("http://publications.europa.eu/resource/authority/data-theme/REGI",),  # Buildings and land.
    "121": (),  # Animals: no stable EU data-theme candidate in the current input mapping.
    "122": ("http://dcat-ap.ch/vocabulary/themes/geography", "http://publications.europa.eu/resource/authority/data-theme/REGI"),
    "123": ("http://dcat-ap.ch/vocabulary/themes/legislation", "http://publications.europa.eu/resource/authority/data-theme/JUST"),
    "124": ("http://dcat-ap.ch/vocabulary/themes/energy", "http://publications.europa.eu/resource/authority/data-theme/ENER"),
    "125": ("http://dcat-ap.ch/vocabulary/themes/statistical-basis", "http://publications.europa.eu/resource/authority/data-theme/GOVE"),
    "126": ("http://dcat-ap.ch/vocabulary/themes/social-security", "http://publications.europa.eu/resource/authority/data-theme/SOCI"),
}


# Explicit EU -> i14y theme candidates.
#
# One-to-one EU themes are mapped directly. One-to-many EU themes are resolved
# by keywords in dcat_properties_utils.get_themes(). If no candidate is confirmed
# by keywords, no i14y theme is emitted for that EU theme.
EU_THEME_TO_I14Y_CANDIDATES = {
    "http://publications.europa.eu/resource/authority/data-theme/AGRI": ("109",),
    "http://publications.europa.eu/resource/authority/data-theme/ENER": ("124",),
    "http://publications.europa.eu/resource/authority/data-theme/ENVI": ("113",),
    "http://publications.europa.eu/resource/authority/data-theme/HEAL": ("114",),
    "http://publications.europa.eu/resource/authority/data-theme/TRAN": ("116",),

    # One-to-many EU themes: resolved through keywords only.
    "http://publications.europa.eu/resource/authority/data-theme/ECON": ("101", "112", "115", "118"),
    "http://publications.europa.eu/resource/authority/data-theme/SOCI": ("102", "106", "117", "126"),
    "http://publications.europa.eu/resource/authority/data-theme/EDUC": ("103", "108"),
    "http://publications.europa.eu/resource/authority/data-theme/JUST": ("105", "123"),
    "http://publications.europa.eu/resource/authority/data-theme/GOVE": ("107", "111", "119", "125"),
    "http://publications.europa.eu/resource/authority/data-theme/REGI": ("120", "122"),
}


# Keyword labels that disambiguate a one-to-many EU theme into one i14y theme.
#
# The aliases include:
# - i14y codelist labels (Labour, Buildings and land, Official statistics, ...)
# - observed DCAT/BFS statistical theme labels from the source keywords
#
# They deliberately exclude broad dimensions such as gender/man/woman and ODIN.
I14Y_THEME_KEYWORD_ALIASES = {
    "101": {
        "labour",
        "work",
        "work and income",
        "travail",
        "travail et remuneration",
        "arbeit",
        "arbeit und erwerb",
        "lavoro",
        "lavoro e reddito",
        "lavur e gudogn",
    },
    "102": {
        "construction",
        "construction and housing",
        "construction et logement",
        "bauen",
        "bau- und wohnungswesen",
        "costruzione",
        "costruzione e abitazioni",
    },
    "103": {
        "education",
        "education and science",
        "education et science",
        "bildung",
        "bildung und wissenschaft",
        "istruzione",
        "formazione e scienza",
    },
    "105": {
        "jurisdiction",
        "crime and criminal justice",
        "criminalite et droit penal",
        "kriminalitat und strafrecht",
        "criminalita e diritto penale",
        "criminalitad e dretg penal",
    },
    "107": {
        "political activities",
        "politics",
        "activites politiques",
        "politique",
        "politische aktivitaten",
        "politik",
        "attivita politiche",
        "politica",
    },
    "108": {
        "culture",
        "culture, media, information society, sports",
        "culture, media, information society, sport",
        "culture, medias, societe de l'information, sport",
        "kultur, medien, informationsgesellschaft, sport",
        "cultura, media, societa dell'informazione, sport",
    },
    "109": {
        "agriculture",
        "agriculture and forestry",
        "agriculture, forestry",
        "agriculture et sylviculture",
        "landwirtschaft",
        "land- und forstwirtschaft",
        "agricoltura",
        "agricoltura e selvicoltura",
    },
    "111": {
        "security",
        "public order and security",
        "securite",
        "ordre public et securite",
        "sicherheit",
        "offentliche ordnung und sicherheit",
        "sicurezza",
        "ordine pubblico e sicurezza",
    },
    "113": {
        "environment",
        "territory and environment",
        "environnement",
        "territoire et environnement",
        "umwelt",
        "raum und umwelt",
        "ambiente",
        "territorio e ambiente",
    },
    "114": {
        "health",
        "sante",
        "gesundheit",
        "salute",
    },
    "115": {
        "economy",
        "economie",
        "wirtschaft",
        "economia",
        "industry and services",
        "industrie et services",
        "industrie und dienstleistungen",
        "industria e servizi",
        "national economy",
        "economie nationale",
        "volkswirtschaft",
        "economia nazionale",
        "prices",
        "prix",
        "preise",
        "prezzi",
        "finances",
        "finanzen",
        "finanze",
        "trade",
        "commerce",
        "handel",
        "commercio",
        "tourism",
        "tourisme",
        "tourismus",
        "turismo",
        "general government and finance",
    },
    "116": {
        "mobility",
        "mobility and transport",
        "mobilite",
        "mobilite et transports",
        "mobilitat",
        "mobilitat und verkehr",
        "mobilita",
        "mobilita e trasporti",
    },
    "117": {
        "citizens",
        "population",
        "habitants",
        "einwohner",
        "bevolkerung",
        "cittadini",
        "popolazione",
    },
    "118": {
        "businesses",
        "industry and services",
        "entreprises",
        "unternehmen",
        "imprese",
        "businesses",
    },
    "119": {
        "authorities",
        "autorites",
        "behorden",
        "autorita",
        "administration",
        "verwaltung",
        "amministrazione",
    },
    "120": {
        "buildings and land",
        "batiments et biens-fonds",
        "gebaude und grundstucke",
        "edifici e abitazioni",
        "buildings and land",
        "batiments et terrains",
        "gebaude und boden",
        "edifici e terreni",
    },
    "122": {
        "geoinformation",
        "geo-informations",
        "geoinformationen",
        "geoinformazione",
        "geography",
        "geographie",
        "geografie",
        "geografia",
    },
    "123": {
        "compilation of laws",
        "recueil du droit",
        "rechtssammlung",
        "raccolta delle basi giuridiche",
        "legislation",
        "gesetzgebung",
        "legislazione",
    },
    "124": {
        "energy",
        "energie",
        "energia",
    },
    "125": {
        "official statistics",
        "statistique publique",
        "offentliche statistik",
        "statistica pubblica",
        "statistical basis",
        "statistical basis and overviews",
        "bases statistiques",
        "bases statistiques et generalites",
        "statistische grundlagen",
        "statistische grundlagen und ubersichten",
        "basi statistiche",
        "basi statistiche e panoramica",
    },
    "126": {
        "social security",
        "securite sociale",
        "soziale sicherheit",
        "sicurezza sociale",
    },
}


MEDIA_TYPE_MAPPING = {
    "https://www.iana.org/assignments/media-types/application/geo+json": "application/geo+json",
    "https://www.iana.org/assignments/media-types/application/gzip": "application/gzip",
    "https://www.iana.org/assignments/media-types/application/json": "application/json",
    "https://www.iana.org/assignments/media-types/application/ld+json": "application/ld+json",
    "https://www.iana.org/assignments/media-types/application/pdf": "application/pdf",
    "https://www.iana.org/assignments/media-types/application/rdf+xml": "application/rdf+xml",
    "https://www.iana.org/assignments/media-types/application/sparql-query": "application/sparql-query",
    "https://www.iana.org/assignments/media-types/application/sql": "application/sql",
    "https://www.iana.org/assignments/media-types/application/vnd.gentoo.gpkg": "application/vnd.gentoo.gpkg",
    "https://www.iana.org/assignments/media-types/application/vnd.rar": "application/vnd.rar",
    "https://www.iana.org/assignments/media-types/application/vnd.shp": "application/vnd.shp",
    "https://www.iana.org/assignments/media-types/application/xml": "application/xml",
    "https://www.iana.org/assignments/media-types/application/yaml": "application/yaml",
    "https://www.iana.org/assignments/media-types/application/zip": "application/zip",
    "https://www.iana.org/assignments/media-types/text/csv": "text/csv",
    "https://www.iana.org/assignments/media-types/text/html": "text/html",
    "https://www.iana.org/assignments/media-types/text/n3": "text/n3",
    "https://www.iana.org/assignments/media-types/text/vnd.gml": "text/vnd.gml",
    "https://www.iana.org/assignments/media-types/text/xml": "text/xml"
}


FORMAT_TYPE_MAPPING =  {
    "https://www.iana.org/assignments/media-types/application/geo+json": "GEOJSON",
    "https://www.iana.org/assignments/media-types/application/gzip": "GZIP",
    "https://www.iana.org/assignments/media-types/application/json": "JSON",
    "https://www.iana.org/assignments/media-types/application/ld+json": "JSON_LD",
    "https://www.iana.org/assignments/media-types/application/pdf": "PDF",
    "https://www.iana.org/assignments/media-types/application/rdf+xml": "RDF_XML",
    "https://www.iana.org/assignments/media-types/application/sparql-query": "SPARQLQ",
    "https://www.iana.org/assignments/media-types/application/sql": "SQL",
    "https://www.iana.org/assignments/media-types/application/vnd.gentoo.gpkg": "GPKG",
    "https://www.iana.org/assignments/media-types/application/vnd.rar": "RAR",
    "https://www.iana.org/assignments/media-types/application/vnd.shp": "SHP",
    "https://www.iana.org/assignments/media-types/application/xml": "XML",
    "https://www.iana.org/assignments/media-types/application/yaml": "YAML",
    "https://www.iana.org/assignments/media-types/application/zip": "ZIP",
    "https://www.iana.org/assignments/media-types/text/csv": "CSV",
    "https://www.iana.org/assignments/media-types/text/html": "HTML",
    "https://www.iana.org/assignments/media-types/text/n3": "N3",
    "https://www.iana.org/assignments/media-types/text/vnd.gml": "GML",
    "https://www.iana.org/assignments/media-types/text/xml": "XML"
}

VALID_FORMAT_CODES = {"CSV", "DXF", "EPUB", "GDB", "GEOJSON", "GEOTIFF", "GIF", "GML", "GPKG", "GPX",
    "HTML", "INTERLIS", "JPEG", "JSON", "JSON_LD", "KML", "MP3", "N3", "ODS", "PDF",
    "PNG", "RDF", "RDF_TURTLE", "RDF_XML", "RSS", "SCHEMA_XML", "SDMX", "SHP", "SKOS_XML",
    "SPARQLQ", "SQL", "SVG", "TIFF", "TSV", "TXT", "WFS_SRVC", "WMS_SRVC", "WMTS_SRVC",
    "XLS", "XLSX", "XML", "YAML"}

VOCAB_EU_PLANNED_AVAILABILITY = {

    "AVAILABLE": ("http://publications.europa.eu/resource/authority/planned-availability/AVAILABLE", "http://data.europa.eu/r5r/availability/available" ), 
    "EXPERIMENTAL": ("http://publications.europa.eu/resource/authority/planned-availability/EXPERIMENTAL", "http://data.europa.eu/r5r/availability/experimental" ),
    "STABLE": ("http://publications.europa.eu/resource/authority/planned-availability/STABLE", "http://data.europa.eu/r5r/availability/stable"),
    "TEMPORARY": ("http://publications.europa.eu/resource/authority/planned-availability/TEMPORARY", "http://data.europa.eu/r5r/availability/temporary"), 
  
}

LANGUAGES_MAPPING = {
    "de": ("de", "DE","http://publications.europa.eu/resource/authority/language/DEU", "http://id.loc.gov/vocabulary/iso639-1/de" ),
    "fr": ("fr","FR","http://publications.europa.eu/resource/authority/language/FRA", "http://id.loc.gov/vocabulary/iso639-1/fr" ),
    "it": ("it", "IT","http://publications.europa.eu/resource/authority/language/ITA", "http://id.loc.gov/vocabulary/iso639-1/it" ),
    "en": ("en", "EN", "http://publications.europa.eu/resource/authority/language/ENG", "http://id.loc.gov/vocabulary/iso639-1/en" )
}
