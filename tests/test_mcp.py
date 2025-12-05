"""Tests for the MCP server module."""

from unittest.mock import Mock, patch

import pytest

from fivetran_mcp_server.src.mcp import FivetranMCPServer


class TestFivetranMCPServer:
    """Test the FivetranMCPServer class."""

    @patch("fivetran_mcp_server.src.mcp.force_reconfigure_all_loggers")
    @patch("fivetran_mcp_server.src.mcp.settings")
    @patch("fivetran_mcp_server.src.mcp.FastMCP")
    @patch("fivetran_mcp_server.src.mcp.logger")
    def test_init_success(
        self, mock_logger, mock_fastmcp, mock_settings, mock_force_reconfigure
    ):
        """Test successful initialization of FivetranMCPServer."""
        # Arrange
        mock_mcp = Mock()
        mock_fastmcp.return_value = mock_mcp
        mock_settings.PYTHON_LOG_LEVEL = "INFO"

        # Act
        server = FivetranMCPServer()

        # Assert
        assert server.mcp == mock_mcp
        mock_logger.info.assert_called_with(
            "Fivetran MCP Server initialized successfully"
        )

    @patch("fivetran_mcp_server.src.mcp.force_reconfigure_all_loggers")
    @patch("fivetran_mcp_server.src.mcp.settings")
    @patch("fivetran_mcp_server.src.mcp.FastMCP")
    @patch("fivetran_mcp_server.src.mcp.logger")
    def test_init_failure(
        self, mock_logger, mock_fastmcp, mock_settings, mock_force_reconfigure
    ):
        """Test initialization failure handling."""
        # Arrange
        mock_fastmcp.side_effect = Exception("Test error")
        mock_settings.PYTHON_LOG_LEVEL = "INFO"

        # Act & Assert
        with pytest.raises(Exception, match="Test error"):
            FivetranMCPServer()

        mock_logger.error.assert_called_with(
            "Failed to initialize Fivetran MCP Server: Test error"
        )

    @patch("fivetran_mcp_server.src.mcp.force_reconfigure_all_loggers")
    @patch("fivetran_mcp_server.src.mcp.settings")
    @patch("fivetran_mcp_server.src.mcp.FastMCP")
    def test_register_mcp_tools(
        self, mock_fastmcp, mock_settings, mock_force_reconfigure
    ):
        """Test MCP tools registration."""
        # Arrange
        mock_mcp = Mock()
        mock_fastmcp.return_value = mock_mcp
        mock_settings.PYTHON_LOG_LEVEL = "INFO"
        server = FivetranMCPServer()

        # Act
        server._register_mcp_tools()

        # Assert - currently no tools registered, just verify method exists
        assert hasattr(server, "_register_mcp_tools")

    def test_server_attributes(self):
        """Test that server has required attributes for tools-first architecture."""
        # Arrange & Act
        with (
            patch("fivetran_mcp_server.src.mcp.settings") as mock_settings,
            patch("fivetran_mcp_server.src.mcp.FastMCP"),
            patch("fivetran_mcp_server.src.mcp.force_reconfigure_all_loggers"),
        ):
            mock_settings.PYTHON_LOG_LEVEL = "INFO"
            server = FivetranMCPServer()

        # Assert
        assert hasattr(server, "mcp")
        assert hasattr(server, "_register_mcp_tools")

    def test_tools_first_architecture_compliance(self):
        """Test that server adheres to tools-first architecture by not having resource/prompt methods."""
        # Arrange & Act
        with (
            patch("fivetran_mcp_server.src.mcp.settings") as mock_settings,
            patch("fivetran_mcp_server.src.mcp.FastMCP"),
            patch("fivetran_mcp_server.src.mcp.force_reconfigure_all_loggers"),
        ):
            mock_settings.PYTHON_LOG_LEVEL = "INFO"
            server = FivetranMCPServer()

        # Assert - These methods should NOT exist in tools-first architecture
        assert not hasattr(server, "_register_mcp_resources"), (
            "_register_mcp_resources should not exist in tools-first architecture"
        )
        assert not hasattr(server, "_register_mcp_prompts"), (
            "_register_mcp_prompts should not exist in tools-first architecture"
        )
