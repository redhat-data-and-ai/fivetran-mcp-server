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


class FivetranAPIError(Exception):
    """Custom exception for Fivetran API errors with helpful context."""

    def __init__(
        self,
        status_code: int,
        message: str,
        hint: str = "",
        docs: str = "",
    ):
        self.status_code = status_code
        self.message = message
        self.hint = hint
        self.docs = docs
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for API responses."""
        return {
            "status": "error",
            "error": self.message,
            "status_code": self.status_code,
            "hint": self.hint,
            "docs": self.docs,
        }


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

    def _handle_error(self, response: httpx.Response, endpoint: str) -> None:
        """Handle HTTP errors with helpful messages.

        Args:
            response: The HTTP response object.
            endpoint: The API endpoint that was called.

        Raises:
            FivetranAPIError: With helpful error message and hints.
        """
        status = response.status_code
        
        error_messages = {
            401: {
                "error": "Authentication failed",
                "hint": "Check FIVETRAN_API_KEY and FIVETRAN_API_SECRET are correct",
                "docs": "https://fivetran.com/docs/rest-api/getting-started",
            },
            403: {
                "error": "Access forbidden",
                "hint": "Your API key may not have permission for this resource",
                "docs": "https://fivetran.com/docs/rest-api/api-reference",
            },
            404: {
                "error": f"Resource not found: {endpoint}",
                "hint": "Check the connector_id, group_id, or agent_id is correct",
                "docs": "https://fivetran.com/docs/rest-api/api-reference",
            },
            429: {
                "error": "Rate limit exceeded",
                "hint": "Too many requests. Wait a moment and try again",
                "docs": "https://fivetran.com/docs/rest-api/api-limits",
            },
            500: {
                "error": "Fivetran server error",
                "hint": "This is a Fivetran-side issue. Try again later",
                "docs": "https://status.fivetran.com",
            },
        }

        if status in error_messages:
            info = error_messages[status]
            raise FivetranAPIError(
                status_code=status,
                message=info["error"],
                hint=info["hint"],
                docs=info["docs"],
            )
        else:
            # Generic error
            try:
                body = response.json()
                message = body.get("message", response.text)
            except Exception:
                message = response.text
            raise FivetranAPIError(
                status_code=status,
                message=f"API error: {message}",
                hint="Check the Fivetran API documentation",
                docs="https://fivetran.com/docs/rest-api/api-reference",
            )

    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a GET request to the Fivetran API.

        Args:
            endpoint: API endpoint (without base URL), e.g., "connectors".
            params: Optional query parameters.

        Returns:
            Dict containing the API response.

        Raises:
            FivetranAPIError: If the request fails with helpful error info.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"GET {url}")

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers, params=params)
            if response.status_code >= 400:
                self._handle_error(response, endpoint)
            return response.json()

    async def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make a POST request to the Fivetran API.

        Args:
            endpoint: API endpoint (without base URL).
            data: Request body data.

        Returns:
            Dict containing the API response.

        Raises:
            FivetranAPIError: If the request fails with helpful error info.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"POST {url}")

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=self.headers, json=data)
            if response.status_code >= 400:
                self._handle_error(response, endpoint)
            return response.json()

    async def patch(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make a PATCH request to the Fivetran API.

        Args:
            endpoint: API endpoint (without base URL).
            data: Request body data.

        Returns:
            Dict containing the API response.

        Raises:
            FivetranAPIError: If the request fails with helpful error info.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"PATCH {url}")

        async with httpx.AsyncClient() as client:
            response = await client.patch(url, headers=self.headers, json=data)
            if response.status_code >= 400:
                self._handle_error(response, endpoint)
            return response.json()

    async def delete(self, endpoint: str) -> Dict[str, Any]:
        """Make a DELETE request to the Fivetran API.

        Args:
            endpoint: API endpoint (without base URL).

        Returns:
            Dict containing the API response.

        Raises:
            FivetranAPIError: If the request fails with helpful error info.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"DELETE {url}")

        async with httpx.AsyncClient() as client:
            response = await client.delete(url, headers=self.headers)
            if response.status_code >= 400:
                self._handle_error(response, endpoint)
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

