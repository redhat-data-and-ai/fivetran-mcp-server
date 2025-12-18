"""Tests for the connectors tools module."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from fivetran_mcp_server.tools.connectors import (
    VALID_AGENT_STATUSES,
    VALID_STATUSES,
    _get_connector_status,
    _get_connector_url,
    _hours_since,
    _paginate,
    _parse_timestamp,
    _resolve_env_to_group_ids,
    diagnose_connector,
    get_connector_schema_status,
    get_hybrid_agent_details,
    get_sync_history,
    list_connectors,
    list_hybrid_agents,
)
from fivetran_mcp_server.fivetran_client import FivetranAPIError


class TestHelperFunctions:
    """Test helper functions."""

    def test_get_connector_url(self):
        """Test generating Fivetran dashboard URL."""
        url = _get_connector_url("abc123")
        assert url == "https://fivetran.com/dashboard/connectors/abc123/status"

    def test_get_connector_url_empty_id(self):
        """Test URL generation with empty ID."""
        url = _get_connector_url("")
        assert url == "https://fivetran.com/dashboard/connectors//status"

    def test_parse_timestamp_valid_iso(self):
        """Test parsing valid ISO timestamp."""
        ts = "2024-01-15T10:30:00Z"
        result = _parse_timestamp(ts)
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_timestamp_with_timezone(self):
        """Test parsing timestamp with timezone offset."""
        ts = "2024-01-15T10:30:00+00:00"
        result = _parse_timestamp(ts)
        assert result is not None
        assert isinstance(result, datetime)

    def test_parse_timestamp_none(self):
        """Test parsing None timestamp."""
        result = _parse_timestamp(None)
        assert result is None

    def test_parse_timestamp_empty_string(self):
        """Test parsing empty string timestamp."""
        result = _parse_timestamp("")
        assert result is None

    def test_parse_timestamp_invalid(self):
        """Test parsing invalid timestamp."""
        result = _parse_timestamp("not-a-timestamp")
        assert result is None

    def test_hours_since_recent(self):
        """Test hours since a recent timestamp."""
        # Create a timestamp from 2 hours ago
        now = datetime.now(timezone.utc)
        two_hours_ago = now.replace(hour=now.hour - 2 if now.hour >= 2 else 22)
        ts = two_hours_ago.isoformat()

        result = _hours_since(ts)
        assert result is not None
        # Allow some flexibility for test execution time
        assert 1.5 <= result <= 2.5 or result >= 21  # Handle day wrap

    def test_hours_since_none(self):
        """Test hours since None timestamp."""
        result = _hours_since(None)
        assert result is None

    def test_hours_since_invalid(self):
        """Test hours since invalid timestamp."""
        result = _hours_since("invalid")
        assert result is None


class TestGetConnectorStatus:
    """Test _get_connector_status function."""

    def test_status_paused(self):
        """Test connector status when paused."""
        connector = {"paused": True, "status": {}}
        assert _get_connector_status(connector) == "paused"

    def test_status_failed_sync_state(self):
        """Test connector status when sync failed."""
        connector = {"paused": False, "status": {"sync_state": "failed"}}
        assert _get_connector_status(connector) == "failed"

    def test_status_failed_rescheduled(self):
        """Test connector status when rescheduled."""
        connector = {"paused": False, "status": {"sync_state": "rescheduled"}}
        assert _get_connector_status(connector) == "failed"

    def test_status_failed_setup_broken(self):
        """Test connector status when setup is broken."""
        connector = {"paused": False, "status": {"setup_state": "broken"}}
        assert _get_connector_status(connector) == "failed"

    def test_status_failed_setup_incomplete(self):
        """Test connector status when setup is incomplete."""
        connector = {"paused": False, "status": {"setup_state": "incomplete"}}
        assert _get_connector_status(connector) == "failed"

    def test_status_warning(self):
        """Test connector status with warnings."""
        connector = {
            "paused": False,
            "status": {"sync_state": "syncing", "warnings": [{"code": "test"}]},
        }
        assert _get_connector_status(connector) == "warning"

    def test_status_healthy(self):
        """Test healthy connector status."""
        connector = {
            "paused": False,
            "status": {
                "sync_state": "syncing",
                "setup_state": "connected",
                "warnings": [],
            },
        }
        assert _get_connector_status(connector) == "healthy"

    def test_status_healthy_no_warnings_key(self):
        """Test healthy connector status without warnings key."""
        connector = {
            "paused": False,
            "status": {"sync_state": "syncing", "setup_state": "connected"},
        }
        assert _get_connector_status(connector) == "healthy"


class TestPaginate:
    """Test _paginate helper function."""

    @pytest.mark.asyncio
    async def test_paginate_single_page(self):
        """Test pagination with single page."""
        mock_client = AsyncMock()
        mock_client.get.return_value = {
            "data": {
                "items": [{"id": "1"}, {"id": "2"}],
                "next_cursor": None,
            }
        }

        result = await _paginate(mock_client, "connectors")

        assert len(result) == 2
        assert result[0]["id"] == "1"
        mock_client.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_paginate_multiple_pages(self):
        """Test pagination with multiple pages."""
        mock_client = AsyncMock()
        mock_client.get.side_effect = [
            {"data": {"items": [{"id": "1"}], "next_cursor": "cursor1"}},
            {"data": {"items": [{"id": "2"}], "next_cursor": "cursor2"}},
            {"data": {"items": [{"id": "3"}], "next_cursor": None}},
        ]

        result = await _paginate(mock_client, "connectors")

        assert len(result) == 3
        assert mock_client.get.call_count == 3

    @pytest.mark.asyncio
    async def test_paginate_empty_response(self):
        """Test pagination with empty response."""
        mock_client = AsyncMock()
        mock_client.get.return_value = {"data": {"items": [], "next_cursor": None}}

        result = await _paginate(mock_client, "connectors")

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_paginate_with_params(self):
        """Test pagination with additional parameters."""
        mock_client = AsyncMock()
        mock_client.get.return_value = {"data": {"items": [], "next_cursor": None}}

        await _paginate(mock_client, "connectors", params={"limit": 100})

        mock_client.get.assert_called_once_with("connectors", params={"limit": 100})


class TestResolveEnvToGroupIds:
    """Test _resolve_env_to_group_ids function."""

    @pytest.mark.asyncio
    async def test_resolve_env_by_group_id(self):
        """Test resolving when env is already a group ID."""
        mock_client = AsyncMock()
        mock_client.get.return_value = {
            "data": {
                "items": [
                    {"id": "group_abc", "name": "Production"},
                    {"id": "group_def", "name": "Development"},
                ],
                "next_cursor": None,
            }
        }

        group_ids, group_names = await _resolve_env_to_group_ids(
            mock_client, "group_abc"
        )

        assert group_ids == ["group_abc"]
        assert group_names == ["Production"]

    @pytest.mark.asyncio
    async def test_resolve_env_by_name_partial_match(self):
        """Test resolving environment by partial name match."""
        mock_client = AsyncMock()
        mock_client.get.return_value = {
            "data": {
                "items": [
                    {"id": "group_1", "name": "company-prod"},
                    {"id": "group_2", "name": "company-dev"},
                    {"id": "group_3", "name": "company-prod-backup"},
                ],
                "next_cursor": None,
            }
        }

        group_ids, group_names = await _resolve_env_to_group_ids(mock_client, "prod")

        assert len(group_ids) == 2
        assert "group_1" in group_ids
        assert "group_3" in group_ids

    @pytest.mark.asyncio
    async def test_resolve_env_case_insensitive(self):
        """Test case-insensitive environment matching."""
        mock_client = AsyncMock()
        mock_client.get.return_value = {
            "data": {
                "items": [{"id": "group_1", "name": "PRODUCTION"}],
                "next_cursor": None,
            }
        }

        group_ids, group_names = await _resolve_env_to_group_ids(
            mock_client, "production"
        )

        assert group_ids == ["group_1"]

    @pytest.mark.asyncio
    async def test_resolve_env_no_match(self):
        """Test when no groups match."""
        mock_client = AsyncMock()
        mock_client.get.return_value = {
            "data": {
                "items": [{"id": "group_1", "name": "Production"}],
                "next_cursor": None,
            }
        }

        group_ids, group_names = await _resolve_env_to_group_ids(
            mock_client, "nonexistent"
        )

        assert group_ids == []
        assert group_names == []


class TestListConnectors:
    """Test list_connectors function."""

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_list_connectors_all(self, mock_get_client):
        """Test listing all connectors."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "items": [
                    {
                        "id": "conn_1",
                        "service": "postgres",
                        "schema": "public",
                        "group_id": "group_1",
                        "paused": False,
                        "status": {
                            "sync_state": "syncing",
                            "setup_state": "connected",
                            "warnings": [],
                        },
                    }
                ],
                "next_cursor": None,
            }
        }

        result = await list_connectors()

        assert result["status"] == "success"
        assert result["count"] == 1
        assert result["connectors"][0]["id"] == "conn_1"
        assert result["connectors"][0]["connector_status"] == "healthy"

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_list_connectors_invalid_status(self, mock_get_client):
        """Test listing connectors with invalid status filter."""
        result = await list_connectors(status="invalid_status")

        assert result["status"] == "error"
        assert "Invalid status" in result["error"]

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_list_connectors_by_status(self, mock_get_client):
        """Test filtering connectors by status."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "items": [
                    {
                        "id": "conn_1",
                        "service": "postgres",
                        "paused": False,
                        "status": {"sync_state": "failed", "warnings": []},
                    },
                    {
                        "id": "conn_2",
                        "service": "mysql",
                        "paused": False,
                        "status": {"sync_state": "syncing", "warnings": []},
                    },
                ],
                "next_cursor": None,
            }
        }

        result = await list_connectors(status="failed")

        assert result["status"] == "success"
        assert result["count"] == 1
        assert result["connectors"][0]["id"] == "conn_1"

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_list_connectors_by_env_no_match(self, mock_get_client):
        """Test filtering by environment with no matches."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "items": [{"id": "group_1", "name": "Production"}],
                "next_cursor": None,
            }
        }

        result = await list_connectors(env="nonexistent")

        assert result["status"] == "error"
        assert "No groups found" in result["error"]

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_list_connectors_api_error(self, mock_get_client):
        """Test handling API errors."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.side_effect = FivetranAPIError(
            status_code=401,
            message="Authentication failed",
            hint="Check credentials",
            docs="https://docs.fivetran.com",
        )

        result = await list_connectors()

        assert result["status"] == "error"
        assert result["status_code"] == 401

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_list_connectors_value_error(self, mock_get_client):
        """Test handling ValueError (missing credentials)."""
        mock_get_client.side_effect = ValueError("Missing API credentials")

        result = await list_connectors()

        assert result["status"] == "error"
        assert "Missing API credentials" in result["error"]


class TestGetConnectorSchemaStatus:
    """Test get_connector_schema_status function."""

    @pytest.mark.asyncio
    async def test_get_schema_status_empty_id(self):
        """Test with empty connector ID."""
        result = await get_connector_schema_status("")

        assert result["status"] == "error"
        assert "connector_id is required" in result["error"]

    @pytest.mark.asyncio
    async def test_get_schema_status_whitespace_id(self):
        """Test with whitespace-only connector ID."""
        result = await get_connector_schema_status("   ")

        assert result["status"] == "error"
        assert "connector_id is required" in result["error"]

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_get_schema_status_success(self, mock_get_client):
        """Test successful schema status retrieval."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "schemas": {
                    "public": {
                        "enabled": True,
                        "tables": {
                            "users": {"enabled": True, "sync_mode": "soft_delete"},
                            "orders": {"enabled": False, "sync_mode": "soft_delete"},
                        },
                    }
                }
            }
        }

        result = await get_connector_schema_status("conn_123")

        assert result["status"] == "success"
        assert result["connector_id"] == "conn_123"
        assert result["schema_count"] == 1
        assert result["summary"]["total_tables"] == 2
        assert result["summary"]["enabled_tables"] == 1
        assert result["summary"]["disabled_tables"] == 1

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_get_schema_status_api_error(self, mock_get_client):
        """Test handling API errors."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.side_effect = FivetranAPIError(
            status_code=404,
            message="Connector not found",
            hint="Check connector ID",
            docs="",
        )

        result = await get_connector_schema_status("invalid_id")

        assert result["status"] == "error"
        assert result["status_code"] == 404


class TestListHybridAgents:
    """Test list_hybrid_agents function."""

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_list_agents_all(self, mock_get_client):
        """Test listing all hybrid agents."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "items": [
                    {
                        "id": "agent_1",
                        "display_name": "Production Agent",
                        "group_id": "group_1",
                        "registered_at": "2024-01-01T00:00:00Z",
                        "usage": [],
                    }
                ],
                "next_cursor": None,
            }
        }

        result = await list_hybrid_agents()

        assert result["status"] == "success"
        assert result["count"] == 1
        assert result["agents"][0]["id"] == "agent_1"

    @pytest.mark.asyncio
    async def test_list_agents_invalid_status(self):
        """Test with invalid status filter."""
        result = await list_hybrid_agents(status="invalid")

        assert result["status"] == "error"
        assert "Invalid status" in result["error"]

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_list_agents_by_status(self, mock_get_client):
        """Test filtering agents by status."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client

        # First call for listing agents
        mock_client.get.side_effect = [
            {
                "data": {
                    "items": [
                        {"id": "agent_1", "group_id": "g1", "usage": []},
                        {"id": "agent_2", "group_id": "g2", "usage": []},
                    ],
                    "next_cursor": None,
                }
            },
            # Agent details calls
            {"data": {"id": "agent_1", "online": True}},
            {"data": {"id": "agent_2", "online": False}},
        ]

        result = await list_hybrid_agents(status="live")

        assert result["status"] == "success"
        assert result["count"] == 1
        assert result["agents"][0]["id"] == "agent_1"


class TestGetHybridAgentDetails:
    """Test get_hybrid_agent_details function."""

    @pytest.mark.asyncio
    async def test_get_agent_details_empty_id(self):
        """Test with empty agent ID."""
        result = await get_hybrid_agent_details("")

        assert result["status"] == "error"
        assert "agent_id is required" in result["error"]

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_get_agent_details_success(self, mock_get_client):
        """Test successful agent details retrieval."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "id": "agent_123",
                "display_name": "My Agent",
                "group_id": "group_1",
                "online": True,
                "registered_at": "2024-01-01T00:00:00Z",
                "usage": [{"connector_id": "conn_1"}],
            }
        }

        result = await get_hybrid_agent_details("agent_123")

        assert result["status"] == "success"
        assert result["agent_id"] == "agent_123"
        assert result["agent_status"] == "live"
        assert result["connector_count"] == 1

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_get_agent_details_offline(self, mock_get_client):
        """Test agent details when offline."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "id": "agent_123",
                "online": False,
                "usage": [],
            }
        }

        result = await get_hybrid_agent_details("agent_123")

        assert result["agent_status"] == "offline"


class TestDiagnoseConnector:
    """Test diagnose_connector function."""

    @pytest.mark.asyncio
    async def test_diagnose_empty_id(self):
        """Test with empty connector ID."""
        result = await diagnose_connector("")

        assert result["status"] == "error"
        assert "connector_id is required" in result["error"]

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_diagnose_healthy_connector(self, mock_get_client):
        """Test diagnosing a healthy connector."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        # Use a recent timestamp to avoid "no recent success" warning
        from datetime import datetime, timezone, timedelta

        recent_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        mock_client.get.side_effect = [
            {
                "data": {
                    "id": "conn_123",
                    "service": "postgres",
                    "schema": "public",
                    "group_id": "group_1",
                    "paused": False,
                    "succeeded_at": recent_time,
                    "failed_at": None,
                    "status": {
                        "sync_state": "syncing",
                        "setup_state": "connected",
                        "warnings": [],
                    },
                }
            },
            {"data": {"schemas": {}}},
        ]

        result = await diagnose_connector("conn_123")

        assert result["status"] == "success"
        assert result["overall_health"] == "healthy"
        assert result["issue_count"] == 0

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_diagnose_paused_connector(self, mock_get_client):
        """Test diagnosing a paused connector."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.side_effect = [
            {
                "data": {
                    "paused": True,
                    "status": {
                        "sync_state": "scheduled",
                        "setup_state": "connected",
                        "warnings": [],
                    },
                }
            },
            {"data": {"schemas": {}}},
        ]

        result = await diagnose_connector("conn_123")

        assert result["overall_health"] == "paused"
        assert any(i["issue"] == "Connector is paused" for i in result["issues"])

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_diagnose_failed_connector(self, mock_get_client):
        """Test diagnosing a failed connector."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.side_effect = [
            {
                "data": {
                    "paused": False,
                    "failed_at": "2024-01-15T10:00:00Z",
                    "status": {
                        "sync_state": "failed",
                        "setup_state": "connected",
                        "warnings": [],
                    },
                }
            },
            {"data": {"schemas": {}}},
        ]

        result = await diagnose_connector("conn_123")

        assert result["overall_health"] == "unhealthy"
        assert any(i["severity"] == "high" for i in result["issues"])

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_diagnose_connector_with_warnings(self, mock_get_client):
        """Test diagnosing a connector with warnings."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.side_effect = [
            {
                "data": {
                    "paused": False,
                    "status": {
                        "sync_state": "syncing",
                        "setup_state": "connected",
                        "warnings": [{"code": "warning_1", "message": "Test warning"}],
                    },
                }
            },
            {"data": {"schemas": {}}},
        ]

        result = await diagnose_connector("conn_123")

        assert result["overall_health"] == "warning"
        assert any("warning" in i["category"] for i in result["issues"])

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_diagnose_broken_setup(self, mock_get_client):
        """Test diagnosing a connector with broken setup."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.side_effect = [
            {
                "data": {
                    "paused": False,
                    "status": {
                        "sync_state": "scheduled",
                        "setup_state": "broken",
                        "warnings": [],
                    },
                }
            },
            {"data": {"schemas": {}}},
        ]

        result = await diagnose_connector("conn_123")

        assert result["overall_health"] == "unhealthy"
        assert any("Setup is broken" in i["issue"] for i in result["issues"])


class TestGetSyncHistory:
    """Test get_sync_history function."""

    @pytest.mark.asyncio
    async def test_get_sync_history_empty_id(self):
        """Test with empty connector ID."""
        result = await get_sync_history("")

        assert result["status"] == "error"
        assert "connector_id is required" in result["error"]

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_get_sync_history_success(self, mock_get_client):
        """Test successful sync history retrieval."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "id": "conn_123",
                "service": "postgres",
                "schema": "public",
                "paused": False,
                "succeeded_at": "2024-01-15T10:00:00Z",
                "failed_at": "2024-01-14T08:00:00Z",
                "sync_started": None,
                "status": {
                    "sync_state": "scheduled",
                    "setup_state": "connected",
                    "update_state": "on_schedule",
                    "is_historical_sync": False,
                    "warnings": [],
                    "tasks": [],
                },
            }
        }

        result = await get_sync_history("conn_123")

        assert result["status"] == "success"
        assert result["connector_id"] == "conn_123"
        assert result["last_syncs"]["last_success"] == "2024-01-15T10:00:00Z"
        assert result["last_syncs"]["last_failure"] == "2024-01-14T08:00:00Z"

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_get_sync_history_with_config(self, mock_get_client):
        """Test sync history with config included."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "id": "conn_123",
                "service": "postgres",
                "paused": False,
                "sync_frequency": 360,
                "schedule_type": "auto",
                "daily_sync_time": None,
                "networking_method": "Directly",
                "local_processing_agent_id": None,
                "status": {"sync_state": "scheduled", "warnings": [], "tasks": []},
            }
        }

        result = await get_sync_history("conn_123", include_config=True)

        assert result["status"] == "success"
        assert "sync_config" in result
        assert result["sync_config"]["sync_frequency"] == 360

    @pytest.mark.asyncio
    @patch("fivetran_mcp_server.tools.connectors.get_fivetran_client")
    async def test_get_sync_history_with_warnings(self, mock_get_client):
        """Test sync history with warnings present."""
        mock_client = AsyncMock()
        mock_get_client.return_value = mock_client
        mock_client.get.return_value = {
            "data": {
                "id": "conn_123",
                "status": {
                    "sync_state": "syncing",
                    "warnings": [
                        {
                            "code": "schema_change",
                            "message": "Schema changed",
                            "details": {},
                        }
                    ],
                    "tasks": [{"code": "reconnect", "message": "Reconnection needed"}],
                },
            }
        }

        result = await get_sync_history("conn_123")

        assert result["warnings"]["count"] == 1
        assert result["warnings"]["details"][0]["code"] == "schema_change"
        assert result["tasks"]["count"] == 1


class TestValidStatusConstants:
    """Test status constants are properly defined."""

    def test_valid_connector_statuses(self):
        """Test valid connector status values."""
        assert "all" in VALID_STATUSES
        assert "failed" in VALID_STATUSES
        assert "healthy" in VALID_STATUSES
        assert "paused" in VALID_STATUSES
        assert "warning" in VALID_STATUSES

    def test_valid_agent_statuses(self):
        """Test valid agent status values."""
        assert "all" in VALID_AGENT_STATUSES
        assert "live" in VALID_AGENT_STATUSES
        assert "offline" in VALID_AGENT_STATUSES
