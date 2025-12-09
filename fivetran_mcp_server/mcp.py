"""Fivetran MCP Server implementation.

This module contains the main Fivetran MCP Server class that provides
tools for MCP clients. It uses FastMCP to register and manage MCP capabilities.
"""

from fastmcp import FastMCP

from fivetran_mcp_server.settings import settings
from fivetran_mcp_server.tools.connectors import (
    diagnose_connector,
    get_connector_schema_status,
    get_hybrid_agent_details,
    list_connectors,
    list_hybrid_agents,
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
        - list_connectors: List connectors (filter by env and status)
        - get_connector_schema_status: See table-level sync status
        - diagnose_connector: Comprehensive health check with recommendations
        - list_hybrid_agents: List all hybrid deployment agents
        - get_hybrid_agent_details: Get details for a specific hybrid agent
        """
        self.mcp.tool()(list_connectors)
        self.mcp.tool()(get_connector_schema_status)
        self.mcp.tool()(diagnose_connector)
        self.mcp.tool()(list_hybrid_agents)
        self.mcp.tool()(get_hybrid_agent_details)

        logger.info("Registered 5 Fivetran troubleshooting tools (read-only)")
