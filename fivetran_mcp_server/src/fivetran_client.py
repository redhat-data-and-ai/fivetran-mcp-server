"""Fivetran API client for the Fivetran MCP Server.

This module provides a client for interacting with the Fivetran REST API.
It handles authentication using Basic Auth with Base64-encoded credentials.

See: https://fivetran.com/docs/rest-api/getting-started
"""

import base64
from typing import Any, Dict, Optional

import httpx

from fivetran_mcp_server.src.settings import settings
from fivetran_mcp_server.utils.pylogger import get_python_logger

logger = get_python_logger()


class FivetranClient:
    """Client for interacting with the Fivetran REST API.

    This client handles authentication and provides methods for making
    GET, POST, PATCH, and DELETE requests to the Fivetran API.

    Attributes:
        base_url: The base URL for the Fivetran API.
        headers: HTTP headers including authentication.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        """Initialize the Fivetran client.

        Args:
            api_key: Fivetran API key. Defaults to settings.FIVETRAN_API_KEY.
            api_secret: Fivetran API secret. Defaults to settings.FIVETRAN_API_SECRET.
            base_url: Fivetran API base URL. Defaults to settings.FIVETRAN_BASE_URL.

        Raises:
            ValueError: If API key or secret is not provided.
        """
        self.api_key = api_key or settings.FIVETRAN_API_KEY
        self.api_secret = api_secret or settings.FIVETRAN_API_SECRET
        self.base_url = (base_url or settings.FIVETRAN_BASE_URL).rstrip("/")

        if not self.api_key or not self.api_secret:
            raise ValueError(
                "FIVETRAN_API_KEY and FIVETRAN_API_SECRET must be set. "
                "Set them as environment variables or pass them to the constructor."
            )

        # Create Basic Auth header
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        self.headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        logger.info("Fivetran client initialized", base_url=self.base_url)

    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a GET request to the Fivetran API.

        Args:
            endpoint: API endpoint (without base URL), e.g., "connectors".
            params: Optional query parameters.

        Returns:
            Dict containing the API response.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"GET {url}")

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()

    async def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make a POST request to the Fivetran API.

        Args:
            endpoint: API endpoint (without base URL).
            data: Request body data.

        Returns:
            Dict containing the API response.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"POST {url}")

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()

    async def patch(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make a PATCH request to the Fivetran API.

        Args:
            endpoint: API endpoint (without base URL).
            data: Request body data.

        Returns:
            Dict containing the API response.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"PATCH {url}")

        async with httpx.AsyncClient() as client:
            response = await client.patch(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()

    async def delete(self, endpoint: str) -> Dict[str, Any]:
        """Make a DELETE request to the Fivetran API.

        Args:
            endpoint: API endpoint (without base URL).

        Returns:
            Dict containing the API response.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"DELETE {url}")

        async with httpx.AsyncClient() as client:
            response = await client.delete(url, headers=self.headers)
            response.raise_for_status()
            return response.json()


# Singleton instance - lazily initialized
_client: Optional[FivetranClient] = None


def get_fivetran_client() -> FivetranClient:
    """Get the Fivetran client singleton.

    Returns:
        FivetranClient instance.

    Raises:
        ValueError: If Fivetran credentials are not configured.
    """
    global _client
    if _client is None:
        _client = FivetranClient()
    return _client

