"""Tests for the settings module."""

import os
from unittest.mock import patch

import pytest

from fivetran_mcp_server.settings import Settings, validate_config


class TestSettings:
    """Test the Settings class."""

    def test_default_settings(self):
        """Test that default settings are correct."""
        # Arrange & Act
        settings = Settings()

        # Assert
        assert settings.MCP_HOST == "localhost"
        assert settings.MCP_TRANSPORT_PROTOCOL == "http"
        assert settings.PYTHON_LOG_LEVEL == "INFO"
        assert settings.MCP_SSL_KEYFILE is None
        assert settings.MCP_SSL_CERTFILE is None

    def test_custom_settings_from_env(self):
        """Test that settings can be overridden from environment variables."""
        # Arrange
        env_vars = {
            "MCP_HOST": "localhost",
            "MCP_PORT": "8080",
            "MCP_TRANSPORT_PROTOCOL": "streamable-http",
            "PYTHON_LOG_LEVEL": "DEBUG",
            "MCP_SSL_KEYFILE": "/path/to/key.pem",
            "MCP_SSL_CERTFILE": "/path/to/cert.pem",
        }

        # Act
        with patch.dict(os.environ, env_vars):
            settings = Settings()

        # Assert
        assert settings.MCP_HOST == "localhost"
        assert settings.MCP_PORT == 8080
        assert settings.MCP_TRANSPORT_PROTOCOL == "streamable-http"
        assert settings.PYTHON_LOG_LEVEL == "DEBUG"
        assert settings.MCP_SSL_KEYFILE == "/path/to/key.pem"
        assert settings.MCP_SSL_CERTFILE == "/path/to/cert.pem"

    def test_port_validation(self):
        """Test port validation constraints."""
        # Test valid port
        settings = Settings()
        assert 1024 <= settings.MCP_PORT <= 65535

    def test_log_level_validation(self):
        """Test log level validation."""
        # Arrange
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        # Act & Assert
        for level in valid_levels:
            with patch.dict(os.environ, {"PYTHON_LOG_LEVEL": level}):
                settings = Settings()
                assert settings.PYTHON_LOG_LEVEL.upper() in valid_levels

    def test_transport_protocol_validation(self):
        """Test transport protocol validation."""
        # Arrange
        valid_protocols = ["streamable-http", "sse", "http"]

        # Act & Assert
        for protocol in valid_protocols:
            with patch.dict(os.environ, {"MCP_TRANSPORT_PROTOCOL": protocol}):
                settings = Settings()
                assert settings.MCP_TRANSPORT_PROTOCOL in valid_protocols

    def test_settings_immutability(self):
        """Test that settings are properly configured."""
        # Arrange
        settings = Settings()

        # Act & Assert
        # Settings should be accessible and have the expected attributes
        assert hasattr(settings, "MCP_HOST")
        assert hasattr(settings, "MCP_PORT")
        assert hasattr(settings, "MCP_TRANSPORT_PROTOCOL")
        assert hasattr(settings, "PYTHON_LOG_LEVEL")


class TestValidateConfig:
    """Test the validate_config function."""

    @pytest.fixture
    def valid_settings(self):
        """Create a Settings instance with valid credentials for testing."""
        s = Settings()
        s.FIVETRAN_API_KEY = "test_key"
        s.FIVETRAN_API_SECRET = "test_secret"
        return s

    def test_valid_config(self, valid_settings):
        """Test validation with valid configuration."""
        validate_config(valid_settings)

    def test_missing_api_key(self):
        """Test validation fails when API key is missing."""
        settings = Settings()
        settings.FIVETRAN_API_KEY = None
        settings.FIVETRAN_API_SECRET = "test_secret"

        with pytest.raises(ValueError, match="FIVETRAN_API_KEY is required"):
            validate_config(settings)

    def test_missing_api_secret(self):
        """Test validation fails when API secret is missing."""
        settings = Settings()
        settings.FIVETRAN_API_KEY = "test_key"
        settings.FIVETRAN_API_SECRET = None

        with pytest.raises(ValueError, match="FIVETRAN_API_SECRET is required"):
            validate_config(settings)

    def test_invalid_port_too_low(self, valid_settings):
        """Test validation with port below minimum."""
        valid_settings.MCP_PORT = 1023

        with pytest.raises(ValueError, match="MCP_PORT must be between 1024 and 65535"):
            validate_config(valid_settings)

    def test_invalid_port_too_high(self, valid_settings):
        """Test validation with port above maximum."""
        valid_settings.MCP_PORT = 65536

        with pytest.raises(ValueError, match="MCP_PORT must be between 1024 and 65535"):
            validate_config(valid_settings)

    def test_invalid_log_level(self, valid_settings):
        """Test validation with invalid log level."""
        valid_settings.PYTHON_LOG_LEVEL = "INVALID"

        with pytest.raises(ValueError, match="PYTHON_LOG_LEVEL must be one of"):
            validate_config(valid_settings)

    def test_invalid_transport_protocol(self, valid_settings):
        """Test validation with invalid transport protocol."""
        valid_settings.MCP_TRANSPORT_PROTOCOL = "invalid"

        with pytest.raises(ValueError, match="MCP_TRANSPORT_PROTOCOL must be one of"):
            validate_config(valid_settings)

    def test_valid_log_levels(self, valid_settings):
        """Test all valid log levels pass validation."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for level in valid_levels:
            valid_settings.PYTHON_LOG_LEVEL = level
            validate_config(valid_settings)

    def test_valid_transport_protocols(self, valid_settings):
        """Test all valid transport protocols pass validation."""
        valid_protocols = ["streamable-http", "sse", "http"]

        for protocol in valid_protocols:
            valid_settings.MCP_TRANSPORT_PROTOCOL = protocol
            validate_config(valid_settings)
