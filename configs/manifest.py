DATA_CATALOG = {
    "SPAIN": {
        "country_code": "ES",
        "currency":  "EUR",
        "engine": "zip_csv",

        "network": {
            "stagger_download": True,
            "min_delay": 2,
            "max_delay": 7

        },

        "delimiter": ";",
        "datasets": {
            "2024": {
                "url": "https://www.fega.gob.es/sites/default/files/files/document/Beneficiarios_municipio_ejercicio-financiero-2024.zip",
                "delimiter": ";",
                "encoding": "ISO-8859-1",
                "has_header": True,
                "schema_version": "v1"
            },
            "2023": {
               "url":  "https://www.fega.gob.es/sites/default/files/files/document/Beneficiarios_municipio_ejercicio_financiero-2023.zip",
               "delimiter": ";",
                "encoding": "ISO-88591",
                "has_header": True,
                "schema_version": "v1"
            }
        }
    },

    "GERMANY": {
        "country_code": "DE",
        "currency": "EUR",
        "engine": "csv",
        "delimiter": ";",
        "datasets": {
            "2024": {
                "url": "https://www.agrarzahlungen.de/fileadmin/afig-csv/impdata2024.csv",
                "delimiter": ";",
                "encoding": "latin-1",
                "has_header": True,
                "schema_version": "v1"
            },
            "2023": {
                "url": "https://www.agrarzahlungen.de/fileadmin/afig-csv/impdata2023.csv",
                "delimiter": ";",
                "encoding": "latin-1",
                "has_header": True,
                "schema_version": "v1"
            }
        }
    },

    "IRELAND": {
        "country_code": "IE",
        "currency": "EUR",
        "engine": "mixed", # csv + excel
        "delimiter": ",",
        "datasets": {
            "2024": {
                "url": "https://opendata.agriculture.gov.ie/dataset/f1a3175a-9c64-4184-9261-0be9a86e7bb1/resource/49f90ead-3013-42dd-99e0-057d792b105d/download/all-capben-2024-1.xlsx",
                "delimiter": ",",
                "encoding": "latin-1",
                "has_header": True,
                "schema_version": "v1"
            },
            "2023": {
                "url": "https://opendata.agriculture.gov.ie/dataset/54d5d194-dd6e-4e21-8803-04058d446bbc/resource/e6c9278a-4bf4-4c60-931b-334192877bcf/download/all-capben-2023.csv",
                "delimiter": ",",
                "encoding": "latin-1",
                "has_header": True,
                "schema_version": "v1"
            }
        }
    },

    "UK": {
        "country_code": "UK",
        "engine": "scraper",
        "currency": "BP",

        "datasets": {
            "2024": {
                "url": "https://cap-payments.defra.gov.uk/download.aspx",
                "delimiter": ",",
                "encoding": "utf-8",
                "headers_required": True, 
                "schema_version": "v1"
            
            },

            "2023": {
                "url": "https://cap-payments.defra.gov.uk/download.aspx",
                "encoding": "utf-8",
                "delimiter": ",",
                "has_header": True,
                "schema_version": "v1"

            }
        }
    },

    #  SCRAPER SOURCES (handled later)
    "NETHERLANDS": {
        "country_code": "NED",
        "currency": "EUR",
        "engine": "scraper",
        "portal": "https://europese-subsidies.rvo.nl/en",

        "datasets": { 
        "2023": {
            "format": "html",
            "schema_version": "v1",
            "has_header": True
        }, 
        "2024": {
            "format": "html",
            "schema_version": "v1",
            "has_header": True,
            
        }
        }
    },

    "FRANCE": {
        "country_code": "FR",
        "currency": "EUR",
        "engine": "scraper",
        "portal": "https://www.telepac.agriculture.gouv.fr/telepac/tbp/accueil/accueil.action",
        "datasets": {
            "2023": {
                "format": "html",
                "schema_version": "v1",
                "has_header": True
            },
            "2024": {
                "format": "html",
                "schema_version": "v1",
                "has_header": True
            }
        }
    }
}