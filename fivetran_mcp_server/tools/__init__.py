"""Tools package for Fivetran MCP server."""

import functools
from typing import Any, Callable, Dict, Optional

from fivetran_mcp_server.fivetran_client import FivetranAPIError
from fivetran_mcp_server.utils.pylogger import get_python_logger

logger = get_python_logger()


def mcp_tool(func: Callable) -> Callable:
    """Decorator that provides consistent error handling for MCP tool functions.

    Catches FivetranAPIError, ValueError, and unexpected exceptions,
    returning a standardized error dict instead of raising.
    """

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        try:
            return await func(*args, **kwargs)
        except FivetranAPIError as e:
            return e.to_dict()
        except ValueError as e:
            logger.error(f"Configuration error in {func.__name__}: {e}")
            return {"status": "error", "error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
            return {"status": "error", "error": str(e)}

    return wrapper


def validate_required_id(
    value: Optional[str], field_name: str
) -> Optional[Dict[str, Any]]:
    """Validate that a required ID field is non-empty.

    Returns an error dict if invalid, None if valid.
    Strips the value in-place considerations are left to the caller.
    """
    if not value or not value.strip():
        return {
            "status": "error",
            "error": f"{field_name} is required and cannot be empty",
        }
    return None
