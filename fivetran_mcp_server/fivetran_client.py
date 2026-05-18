"""Fivetran API client for the Fivetran MCP Server.

This module provides a client for interacting with the Fivetran REST API.
It handles authentication using Basic Auth with Base64-encoded credentials,
with automatic retry for transient errors and rate-limit awareness.

See: https://fivetran.com/docs/rest-api/getting-started
"""

import asyncio
import base64
from typing import Any, Dict, Optional

import httpx

from fivetran_mcp_server.settings import settings
from fivetran_mcp_server.utils.pylogger import get_python_logger

logger = get_python_logger()

DEFAULT_TIMEOUT = httpx.Timeout(
    connect=10.0,
    read=30.0,
    write=10.0,
    pool=5.0,
)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 1.0


class FivetranAPIError(Exception):
    """Custom exception for Fivetran API errors with helpful context."""

    def __init__(
        self,
        status_code: int,
        message: str,
        hint: str = "",
        docs: str = "",
    ):
        """Initialize the FivetranAPIError with status code and message."""
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

    Uses a long-lived httpx.AsyncClient for connection pooling and
    explicit timeouts. The client should be closed when no longer needed
    via the `aclose()` method.

    Attributes:
        base_url: The base URL for the Fivetran API.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: Optional[httpx.Timeout] = None,
    ):
        """Initialize the Fivetran client.

        Args:
            api_key: Fivetran API key. Defaults to settings.FIVETRAN_API_KEY.
            api_secret: Fivetran API secret. Defaults to settings.FIVETRAN_API_SECRET.
            base_url: Fivetran API base URL. Defaults to settings.FIVETRAN_BASE_URL.
            timeout: Custom timeout configuration. Defaults to DEFAULT_TIMEOUT.

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

        credentials = f"{self.api_key}:{self.api_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=timeout or DEFAULT_TIMEOUT,
        )

        logger.info("Fivetran client initialized", base_url=self.base_url)

    async def aclose(self) -> None:
        """Close the underlying HTTP client and release connections."""
        await self._client.aclose()

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        endpoint: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute an HTTP request with retry logic for transient failures.

        Retries on 429 (rate limit) and 5xx errors with exponential backoff.
        For 429 responses, respects the Retry-After header if present.
        """
        last_response: Optional[httpx.Response] = None

        for attempt in range(MAX_RETRIES + 1):
            response = await self._client.request(method, url, **kwargs)
            last_response = response

            if response.status_code < 400:
                return response

            if response.status_code not in RETRYABLE_STATUS_CODES:
                break

            if attempt == MAX_RETRIES:
                break

            backoff = self._calculate_backoff(response, attempt)
            logger.warning(
                f"Retryable error {response.status_code} on {method} {endpoint}, "
                f"attempt {attempt + 1}/{MAX_RETRIES}, retrying in {backoff:.1f}s"
            )
            await asyncio.sleep(backoff)

        assert last_response is not None
        return last_response

    def _calculate_backoff(self, response: httpx.Response, attempt: int) -> float:
        """Calculate backoff duration, respecting Retry-After header for 429s."""
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    return float(retry_after)
                except ValueError:
                    pass
        return INITIAL_BACKOFF_SECONDS * (2**attempt)

    def _parse_response_json(
        self, response: httpx.Response, endpoint: str
    ) -> Dict[str, Any]:
        """Safely parse JSON from a response, handling empty or non-JSON bodies."""
        if response.status_code == 204 or not response.content:
            return {"status": "success"}

        try:
            return response.json()
        except Exception:
            raise FivetranAPIError(
                status_code=response.status_code,
                message=f"Invalid JSON response from {endpoint}",
                hint="The API returned a non-JSON response body",
                docs="https://fivetran.com/docs/rest-api/api-reference",
            )

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

    async def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
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

        response = await self._request_with_retry("GET", url, endpoint, params=params)
        if response.status_code >= 400:
            self._handle_error(response, endpoint)
        return self._parse_response_json(response, endpoint)

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

        response = await self._request_with_retry("POST", url, endpoint, json=data)
        if response.status_code >= 400:
            self._handle_error(response, endpoint)
        return self._parse_response_json(response, endpoint)

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

        response = await self._request_with_retry("PATCH", url, endpoint, json=data)
        if response.status_code >= 400:
            self._handle_error(response, endpoint)
        return self._parse_response_json(response, endpoint)

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

        response = await self._request_with_retry("DELETE", url, endpoint)
        if response.status_code >= 400:
            self._handle_error(response, endpoint)
        return self._parse_response_json(response, endpoint)


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
