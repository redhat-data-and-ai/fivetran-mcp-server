"""Fivetran connector tools for troubleshooting and diagnostics (read-only).

This module provides read-only MCP tools for diagnosing Fivetran connector issues.
Works with any Fivetran account - filter connectors by group_id.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fivetran_mcp_server.fivetran_client import (
    FivetranAPIError,
    get_fivetran_client,
)
from fivetran_mcp_server.utils.pylogger import get_python_logger

logger = get_python_logger()

# Fivetran dashboard base URL for connector links
FIVETRAN_DASHBOARD_URL = "https://fivetran.com/dashboard/connectors"


def _get_connector_url(connector_id: str) -> str:
    """Generate Fivetran dashboard URL for a connector."""
    return f"{FIVETRAN_DASHBOARD_URL}/{connector_id}/status"


async def _paginate(
    client: Any, endpoint: str, params: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Helper to fetch all pages from a paginated Fivetran API endpoint.

    Args:
        client: FivetranClient instance.
        endpoint: API endpoint to fetch from.
        params: Optional additional query parameters.

    Returns:
        List of all items from all pages.
    """
    all_items: List[Dict[str, Any]] = []
    cursor = None

    while True:
        request_params = {**(params or {})}
        if cursor:
            request_params["cursor"] = cursor

        response = await client.get(
            endpoint, params=request_params if request_params else None
        )
        data = response.get("data", {})
        items = data.get("items", [])
        all_items.extend(items)

        cursor = data.get("next_cursor")
        if not cursor:
            break

    return all_items


async def list_groups() -> Dict[str, Any]:
    """List all Fivetran groups (destinations).

    Retrieves all destination groups in your Fivetran account.
    Use this to find group IDs for filtering connectors.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - groups: List of groups with id, name, created_at
            - count: Total number of groups
    """
    try:
        client = get_fivetran_client()
        groups = await _paginate(client, "groups")

        processed = [
            {
                "id": g.get("id"),
                "name": g.get("name"),
                "created_at": g.get("created_at"),
            }
            for g in groups
        ]

        logger.info(f"Listed {len(processed)} groups")

        return {
            "status": "success",
            "groups": processed,
            "count": len(processed),
        }

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error listing groups: {e}")
        return {"status": "error", "error": str(e)}


async def list_connectors(group_id: Optional[str] = None) -> Dict[str, Any]:
    """List Fivetran connectors, optionally filtered by group.

    Retrieves connectors from your Fivetran account. Use group_id to filter
    to a specific destination/group.

    Args:
        group_id: Optional group ID to filter connectors. Use list_groups()
                  to find available group IDs.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - connectors: List of connector summaries
            - count: Number of connectors returned
            - group_id: The group filter applied (if any)

    Examples:
        list_connectors()  # All connectors
        list_connectors(group_id="abc123")  # Connectors in specific group
    """
    try:
        client = get_fivetran_client()

        if group_id:
            # Fetch from specific group (with pagination)
            endpoint = f"groups/{group_id}/connectors"
            filter_desc = f"group_id={group_id}"
        else:
            # Fetch all connectors (with pagination)
            endpoint = "connectors"
            filter_desc = "none"

        all_connectors = await _paginate(client, endpoint)

        # Simplify connector info
        simplified = []
        for c in all_connectors:
            connector_id = c.get("id", "")
            simplified.append(
                {
                    "id": connector_id,
                    "service": c.get("service"),
                    "schema": c.get("schema"),
                    "group_id": c.get("group_id"),
                    "paused": c.get("paused", False),
                    "dashboard_url": _get_connector_url(connector_id),
                }
            )

        logger.info(f"Listed {len(simplified)} connectors (filters: {filter_desc})")

        return {
            "status": "success",
            "connectors": simplified,
            "count": len(simplified),
            "group_id": group_id,
        }

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error listing connectors: {e}")
        return {"status": "error", "error": str(e)}


async def list_failed_connectors(group_id: Optional[str] = None) -> Dict[str, Any]:
    """List connectors with failures or warnings, optionally filtered by group.

    Quickly identifies problem connectors by filtering for those with:
    - Failed or rescheduled sync state
    - Broken or incomplete setup state
    - Active warnings

    Args:
        group_id: Optional group ID to filter. Use list_groups() to find IDs.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - failed_connectors: List of connectors with issues
            - count: Number of problem connectors found
            - group_id: The group filter applied (if any)
    """
    try:
        client = get_fivetran_client()

        # Fetch connectors (with pagination)
        if group_id:
            endpoint = f"groups/{group_id}/connectors"
        else:
            endpoint = "connectors"

        all_connectors = await _paginate(client, endpoint)

        failed = []
        for c in all_connectors:
            status_info = c.get("status", {})
            sync_state = status_info.get("sync_state", "")
            setup_state = status_info.get("setup_state", "")
            warnings = status_info.get("warnings", [])

            # Check for actual problems (not just paused)
            has_issues = (
                sync_state in ["failed", "rescheduled"]
                or setup_state in ["broken", "incomplete"]
                or len(warnings) > 0
            )

            if has_issues:
                connector_id = c.get("id", "")
                failed.append(
                    {
                        "id": connector_id,
                        "service": c.get("service"),
                        "schema": c.get("schema"),
                        "group_id": c.get("group_id"),
                        "sync_state": sync_state,
                        "setup_state": setup_state,
                        "warning_count": len(warnings),
                        "warnings": warnings[:3],
                        "dashboard_url": _get_connector_url(connector_id),
                    }
                )

        logger.info(f"Found {len(failed)} connectors with issues")

        return {
            "status": "success",
            "failed_connectors": failed,
            "count": len(failed),
            "group_id": group_id,
        }

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error listing failed connectors: {e}")
        return {"status": "error", "error": str(e)}


async def get_connector_schema_status(connector_id: str) -> Dict[str, Any]:
    """Get table-level sync status for a connector (for troubleshooting).

    Shows which schemas and tables are enabled, syncing, or have issues.
    Useful for identifying specific tables that may be causing problems.

    Args:
        connector_id: The unique identifier for the connector.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - connector_id: The connector ID
            - schemas: List of schemas with their tables and sync status
    """
    try:
        client = get_fivetran_client()
        response = await client.get(f"connectors/{connector_id}/schemas")

        schemas_data = response.get("data", {}).get("schemas", {})

        schemas_list: List[Dict[str, Any]] = []
        for schema_name, schema_info in schemas_data.items():
            tables_list = []
            tables = schema_info.get("tables", {})

            for table_name, table_info in tables.items():
                tables_list.append(
                    {
                        "name": table_name,
                        "enabled": table_info.get("enabled", False),
                        "sync_mode": table_info.get("sync_mode"),
                    }
                )

            schemas_list.append(
                {
                    "name": schema_name,
                    "enabled": schema_info.get("enabled", False),
                    "tables": tables_list,
                    "table_count": len(tables_list),
                }
            )

        logger.info(f"Retrieved schema status for connector: {connector_id}")

        return {
            "status": "success",
            "connector_id": connector_id,
            "dashboard_url": _get_connector_url(connector_id),
            "schemas": schemas_list,
            "schema_count": len(schemas_list),
        }

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error getting schema status for {connector_id}: {e}")
        return {"status": "error", "error": str(e)}


async def list_hybrid_agents() -> Dict[str, Any]:
    """List all Hybrid Deployment Agents and their status.

    Shows all Local Processing Agents (hybrid agents) in your account,
    including their connection status, version, and health.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - agents: List of agents with id, display_name, registered_at, status
            - count: Total number of agents
    """
    try:
        client = get_fivetran_client()
        all_agents = await _paginate(client, "local-processing-agents")

        processed = []
        for agent in all_agents:
            processed.append(
                {
                    "id": agent.get("id"),
                    "display_name": agent.get("display_name"),
                    "group_id": agent.get("group_id"),
                    "registered_at": agent.get("registered_at"),
                    "usage": agent.get("usage", []),
                }
            )

        logger.info(f"Listed {len(processed)} hybrid agents")

        return {
            "status": "success",
            "agents": processed,
            "count": len(processed),
        }

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error listing hybrid agents: {e}")
        return {"status": "error", "error": str(e)}


async def get_hybrid_agent_details(agent_id: str) -> Dict[str, Any]:
    """Get detailed status for a specific Hybrid Deployment Agent.

    Retrieves comprehensive information about a hybrid agent including
    connection status, version, and assigned connectors.

    Args:
        agent_id: The unique identifier for the hybrid agent.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - agent_id: The agent ID
            - display_name: Human-readable agent name
            - group_id: Associated group/destination
            - registered_at: When the agent was registered
            - usage: List of connectors using this agent
    """
    try:
        client = get_fivetran_client()
        response = await client.get(f"local-processing-agents/{agent_id}")

        agent = response.get("data", {})

        logger.info(f"Retrieved details for hybrid agent: {agent_id}")

        return {
            "status": "success",
            "agent_id": agent_id,
            "display_name": agent.get("display_name"),
            "group_id": agent.get("group_id"),
            "registered_at": agent.get("registered_at"),
            "files": agent.get("files", []),
            "usage": agent.get("usage", []),
        }

    except FivetranAPIError as e:
        return e.to_dict()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error getting hybrid agent {agent_id}: {e}")
        return {"status": "error", "error": str(e)}


def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string to datetime."""
    if not ts:
        return None
    try:
        # Handle various ISO formats
        ts = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _hours_since(ts: Optional[str]) -> Optional[float]:
    """Calculate hours since a timestamp."""
    dt = _parse_timestamp(ts)
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    delta = now - dt
    return round(delta.total_seconds() / 3600, 1)


async def diagnose_connector(connector_id: str) -> Dict[str, Any]:
    """Comprehensive health check for a connector with recommendations.

    Analyzes connector state, sync status, warnings, and schema configuration
    to provide a diagnosis with actionable recommendations.

    Args:
        connector_id: The unique identifier for the connector.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - connector_id: The connector ID
            - overall_health: "healthy", "warning", "unhealthy", or "paused"
            - summary: Key connector info
            - issues: List of issues with severity and recommendations
            - checks: Detailed check results
    """
    try:
        client = get_fivetran_client()

        # Get connector details (1 API call)
        response = await client.get(f"connectors/{connector_id}")
        connector = response.get("data", {})
        status_info = connector.get("status", {})

        # Get schema status (1 API call)
        schema_response = await client.get(f"connectors/{connector_id}/schemas")
        schemas_data = schema_response.get("data", {}).get("schemas", {})

        # Extract key info
        sync_state = status_info.get("sync_state", "")
        setup_state = status_info.get("setup_state", "")
        paused = connector.get("paused", False)
        warnings = status_info.get("warnings", [])
        succeeded_at = connector.get("succeeded_at")
        failed_at = connector.get("failed_at")

        # Count tables
        total_tables = 0
        enabled_tables = 0
        for schema_info in schemas_data.values():
            tables = schema_info.get("tables", {})
            for table_info in tables.values():
                total_tables += 1
                if table_info.get("enabled", False):
                    enabled_tables += 1
        disabled_tables = total_tables - enabled_tables

        # Calculate time since events
        hours_since_success = _hours_since(succeeded_at)
        hours_since_failure = _hours_since(failed_at)

        # Build issues list
        issues: List[Dict[str, Any]] = []

        # Check: Paused
        if paused:
            issues.append(
                {
                    "severity": "info",
                    "category": "status",
                    "issue": "Connector is paused",
                    "recommendation": "Resume the connector when ready to sync",
                }
            )

        # Check: Setup state
        if setup_state in ["broken", "incomplete"]:
            issues.append(
                {
                    "severity": "high",
                    "category": "setup",
                    "issue": f"Setup is {setup_state}",
                    "recommendation": "Complete connector setup in Fivetran dashboard",
                }
            )

        # Check: Sync state
        if sync_state == "failed":
            issues.append(
                {
                    "severity": "high",
                    "category": "sync",
                    "issue": "Last sync failed",
                    "details": f"Failed {hours_since_failure} hours ago"
                    if hours_since_failure
                    else "Recently failed",
                    "recommendation": "Check Fivetran logs for error details. Common causes: auth expired, network issues, source unavailable",
                }
            )
        elif sync_state == "rescheduled":
            issues.append(
                {
                    "severity": "medium",
                    "category": "sync",
                    "issue": "Sync was rescheduled",
                    "recommendation": "Fivetran rescheduled due to a transient issue. Monitor next sync",
                }
            )

        # Check: Warnings
        if warnings:
            issues.append(
                {
                    "severity": "medium",
                    "category": "warnings",
                    "issue": f"{len(warnings)} active warning(s)",
                    "details": warnings[:3],  # First 3 warnings
                    "recommendation": "Review and resolve warnings in Fivetran dashboard",
                }
            )

        # Check: Recent failure
        if hours_since_failure and hours_since_failure < 24 and sync_state != "failed":
            issues.append(
                {
                    "severity": "low",
                    "category": "history",
                    "issue": f"Had a failure {hours_since_failure} hours ago",
                    "recommendation": "Monitor for recurring issues",
                }
            )

        # Check: No recent success
        if hours_since_success and hours_since_success > 48 and not paused:
            issues.append(
                {
                    "severity": "medium",
                    "category": "sync",
                    "issue": f"No successful sync in {hours_since_success} hours",
                    "recommendation": "Check if connector is stuck or having issues",
                }
            )

        # Check: Many disabled tables
        if total_tables > 0 and disabled_tables > total_tables * 0.5:
            issues.append(
                {
                    "severity": "low",
                    "category": "schema",
                    "issue": f"{disabled_tables} of {total_tables} tables disabled",
                    "recommendation": "Verify table selection is intentional",
                }
            )

        # Determine overall health
        if paused:
            overall_health = "paused"
        elif any(i["severity"] == "high" for i in issues):
            overall_health = "unhealthy"
        elif any(i["severity"] == "medium" for i in issues):
            overall_health = "warning"
        else:
            overall_health = "healthy"

        logger.info(f"Diagnosed connector {connector_id}: {overall_health}")

        return {
            "status": "success",
            "connector_id": connector_id,
            "dashboard_url": _get_connector_url(connector_id),
            "overall_health": overall_health,
            "summary": {
                "service": connector.get("service"),
                "schema": connector.get("schema"),
                "group_id": connector.get("group_id"),
                "sync_state": sync_state,
                "setup_state": setup_state,
                "paused": paused,
                "last_success": succeeded_at,
                "last_failure": failed_at,
                "hours_since_success": hours_since_success,
                "hours_since_failure": hours_since_failure,
            },
            "issues": issues,
            "issue_count": len(issues),
            "checks": {
                "is_paused": paused,
                "is_syncing": sync_state == "syncing",
                "setup_complete": setup_state == "connected",
                "has_warnings": len(warnings) > 0,
                "has_recent_failure": hours_since_failure is not None
                and hours_since_failure < 24,
                "tables_total": total_tables,
                "tables_enabled": enabled_tables,
                "tables_disabled": disabled_tables,
            },
        }

    except FivetranAPIError as e:
        return e.to_dict()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error diagnosing connector {connector_id}: {e}")
        return {"status": "error", "error": str(e)}
