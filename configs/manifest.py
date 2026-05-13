DATA_CATALOG = {
    "SPAIN": {
        "engine": "zip_csv",
        "delimiter": ";",
        "datasets": {
            "2024": "https://www.fega.gob.es/sites/default/files/files/document/Beneficiarios_municipio_ejercicio-financiero-2024.zip",
            "2023": "https://www.fega.gob.es/sites/default/files/files/document/Beneficiarios_municipio_ejercicio_financiero-2023.zip",
        }
    },

    "GERMANY": {
        "engine": "csv",
        "delimiter": ";",
        "datasets": {
            "2024": "https://www.agrarzahlungen.de/fileadmin/afig-csv/impdata2024.csv",
            "2023": "https://www.agrarzahlungen.de/fileadmin/afig-csv/impdata2023.csv",
        }
    },

    "IRELAND": {
        "engine": "mixed", # csv + excel
        "delimiter": ",",
        "datasets": {
            "2024": "https://opendata.agriculture.gov.ie/dataset/f1a3175a-9c64-4184-9261-0be9a86e7bb1/resource/49f90ead-3013-42dd-99e0-057d792b105d/download/all-capben-2024-1.xlsx",
            "2023": "https://opendata.agriculture.gov.ie/dataset/54d5d194-dd6e-4e21-8803-04058d446bbc/resource/e6c9278a-4bf4-4c60-931b-334192877bcf/download/all-capben-2023.csv",
        }
    },

    "UK": {
        "engine": "excel",
        "currency": "GBP",
        "datasets": {
            "2024": "https://cap-payments.defra.gov.uk/Download/CAP_Payments_2024.xlsx",
            "2023": "https://cap-payments.defra.gov.uk/Download/CAP_Payments_2023.xlsx",
        }
    },

    #  SCRAPER SOURCES (handled later)
    "NETHERLANDS": {
        "engine": "scraper",
        "portal": "https://europese-subsidies.rvo.nl/en",
        "target_years": ["2023", "2024"]
    },

    "FRANCE": {
        "engine": "scraper",
        "portal": "https://www.telepac.agriculture.gouv.fr/telepac/tbp/accueil/accueil.action",
        "target_years": ["2023", "2024"]
    }
}