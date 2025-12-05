"""Fivetran MCP Server implementation.

This module contains the main Fivetran MCP Server class that provides
tools for MCP clients. It uses FastMCP to register and manage MCP capabilities.
"""

from fastmcp import FastMCP

from fivetran_mcp_server.src.settings import settings
from fivetran_mcp_server.src.tools.connectors import (
    get_connector_details,
    get_connector_schema_status,
    list_connectors,
    list_failed_connectors,
    list_groups,
)
from fivetran_mcp_server.utils.pylogger import (
    force_reconfigure_all_loggers,
    get_python_logger,
)

logger = get_python_logger()


class FivetranMCPServer:
    """Main Fivetran MCP Server implementation following tools-first architecture.

    This server provides read-only tools for troubleshooting Fivetran connectors.
    """

    def __init__(self):
        """Initialize the MCP server with Fivetran troubleshooting tools."""
        try:
            # Initialize FastMCP server
            self.mcp = FastMCP("fivetran")

            # Force reconfigure all loggers after FastMCP initialization to ensure structured logging
            force_reconfigure_all_loggers(settings.PYTHON_LOG_LEVEL)

            self._register_mcp_tools()

            logger.info("Fivetran MCP Server initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Fivetran MCP Server: {e}")
            raise

    def _register_mcp_tools(self) -> None:
        """Register MCP tools for Fivetran troubleshooting (read-only).

        Tools:
        - list_groups: List all groups/destinations
        - list_connectors: List connectors (filter by group_id)
        - get_connector_details: Get full details for troubleshooting
        - list_failed_connectors: Find connectors with issues (filter by group_id)
        - get_connector_schema_status: See table-level sync status
        """
        self.mcp.tool()(list_groups)
        self.mcp.tool()(list_connectors)
        self.mcp.tool()(get_connector_details)
        self.mcp.tool()(list_failed_connectors)
        self.mcp.tool()(get_connector_schema_status)

        logger.info("Registered 5 Fivetran troubleshooting tools (read-only)")
