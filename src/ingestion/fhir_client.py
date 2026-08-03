from typing import Any, Dict, List, Optional

import requests

from config.settings import DEFAULT_HEADERS, FHIR_BASE_URL


class FhirClient:
    def __init__(self, base_url: str = FHIR_BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def health_check(self) -> Dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/metadata",
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def search(
        self,
        resource_type: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/{resource_type}",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def read(
        self,
        resource_type: str,
        resource_id: str,
    ) -> Dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/{resource_type}/{resource_id}",
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def search_all(
        self,
        resource_type: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        bundle = self.search(resource_type, params=params)
        entries = self.get_entries(bundle)

        next_url = self.get_next_link(bundle)

        while next_url:
            response = self.session.get(next_url, timeout=30)
            response.raise_for_status()

            bundle = response.json()
            entries.extend(self.get_entries(bundle))
            next_url = self.get_next_link(bundle)

        return entries

    @staticmethod
    def get_entries(bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        return bundle.get("entry", [])

    @staticmethod
    def get_next_link(bundle: Dict[str, Any]) -> Optional[str]:
        for link in bundle.get("link", []):
            if link.get("relation") == "next":
                return link.get("url")

        return None