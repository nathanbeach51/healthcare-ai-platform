import requests

from config.settings import DEFAULT_HEADERS, FHIR_BASE_URL


class FhirClient:

    def __init__(self, base_url=FHIR_BASE_URL):
        self.base_url = base_url

        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def health_check(self):
        response = self.session.get(f"{self.base_url}/metadata")
        response.raise_for_status()
        return response.json()

    def search(self, resource_type, params=None):

        response = self.session.get(
            f"{self.base_url}/{resource_type}",
            params=params
        )

        response.raise_for_status()

        return response.json()

    def read(self, resource_type, resource_id):

        response = self.session.get(
            f"{self.base_url}/{resource_type}/{resource_id}"
        )

        response.raise_for_status()

        return response.json()

    @staticmethod
    def get_entries(bundle):
        return bundle.get("entry", [])