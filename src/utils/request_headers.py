DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept":
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/webp,*/*;q=0.8",
        
    "Accept-Language": "en-GB,en;q=0.9",
    "Connection": "keep-alive",
}

COUNTRY_HEADERS = {

    "UK": {
        "Referer": "https://cap-payments.defra.gov.uk/"
    },

    "IRELAND": {
        "Referer": "https://opendata.agriculture.gov.ie/"
    },

    "SPAIN": {
        "Referer": "https://www.fega.gob.es/"
    },

    "GERMANY" : {
        "Referer": "https://www.agrarzahlungen.de"
    },

    "NETHERLANDS": {
        "Referer" : "https://europese-subsidies.rvo.nl/en"

    }
}


def build_headers(country: str) -> dict:

    headers = DEFAULT_HEADERS.copy()

    headers.update(
        COUNTRY_HEADERS.get(country.upper(), {})
    )

    return headers