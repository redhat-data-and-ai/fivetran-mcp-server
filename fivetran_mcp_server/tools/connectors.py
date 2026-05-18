"""Fivetran connector tools for troubleshooting and diagnostics (read-only).

This module provides read-only MCP tools for diagnosing Fivetran connector issues.
Works with any Fivetran account - filter connectors by env and status.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fivetran_mcp_server.fivetran_client import (
    FivetranAPIError,
    FivetranClient,
    get_fivetran_client,
)
from fivetran_mcp_server.tools import mcp_tool, validate_required_id
from fivetran_mcp_server.utils.pylogger import get_python_logger

logger = get_python_logger()

# Fivetran dashboard base URL for connector links
FIVETRAN_DASHBOARD_URL = "https://fivetran.com/dashboard/connectors"

# Valid status filter values
VALID_STATUSES = {"all", "failed", "healthy", "paused", "warning"}


def _get_connector_url(connector_id: str) -> str:
    """Generate Fivetran dashboard URL for a connector."""
    return f"{FIVETRAN_DASHBOARD_URL}/{connector_id}/status"


async def _paginate(
    client: FivetranClient, endpoint: str, params: Optional[Dict[str, Any]] = None
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


async def _resolve_env_to_group_ids(
    client: FivetranClient, env: str
) -> tuple[List[str], List[str]]:
    """Resolve environment name to group ID(s).

    Args:
        client: FivetranClient instance.
        env: Environment name (e.g., "dev", "prod", "preprod", "sandbox")
             or a group ID directly.

    Returns:
        Tuple of (group_ids, group_names) that match the env filter.
    """
    groups = await _paginate(client, "groups")

    # Check if env is already a group ID
    for g in groups:
        if g.get("id") == env:
            return [env], [g.get("name", "")]

    # Search by partial match on group name (case-insensitive)
    env_lower = env.lower()
    matching_ids = []
    matching_names = []
    for g in groups:
        name = g.get("name", "")
        if env_lower in name.lower():
            matching_ids.append(g.get("id", ""))
            matching_names.append(name)

    return matching_ids, matching_names


def _get_connector_status(connector: Dict[str, Any]) -> str:
    """Determine the status category for a connector.

    Args:
        connector: Raw connector data from Fivetran API.

    Returns:
        Status string: "failed", "warning", "paused", or "healthy"
    """
    status_info = connector.get("status", {})
    sync_state = status_info.get("sync_state", "")
    setup_state = status_info.get("setup_state", "")
    warnings = status_info.get("warnings", [])
    paused = connector.get("paused", False)

    if paused:
        return "paused"
    if sync_state in ["failed", "rescheduled"] or setup_state in [
        "broken",
        "incomplete",
    ]:
        return "failed"
    if warnings:
        return "warning"
    return "healthy"


@mcp_tool
async def list_connectors(
    env: Optional[str] = None, status: Optional[str] = None
) -> Dict[str, Any]:
    """List Fivetran connectors, filtered by environment and/or status.

    Retrieves connectors from your Fivetran account with flexible filtering.

    Args:
        env: Optional environment filter. Use human-readable names like "dev",
             "preprod", "prod", "sandbox" - matches against group names.
             Can also use exact group ID.
        status: Optional status filter. One of:
             - "all": All connectors (default)
             - "failed": Connectors with failures or broken setup
             - "healthy": Connectors working normally
             - "paused": Paused connectors
             - "warning": Connectors with warnings

    Returns:
        Dict containing:
            - status: "success" or "error"
            - connectors: List of connector summaries with status info
            - count: Number of connectors returned
            - filters: The filters applied (env, status)

    Examples:
        list_connectors()                              # All connectors
        list_connectors(env="prod")                    # All prod connectors
        list_connectors(status="failed")              # All failed connectors
        list_connectors(env="dev", status="failed")   # Failed dev connectors
    """
    client = get_fivetran_client()

    # Validate status filter
    status_filter = (status or "all").lower()
    if status_filter not in VALID_STATUSES:
        return {
            "status": "error",
            "error": f"Invalid status '{status}'. Must be one of: {', '.join(VALID_STATUSES)}",
        }

    # Resolve environment to group IDs
    group_ids: List[str] = []
    env_names: List[str] = []
    if env:
        group_ids, env_names = await _resolve_env_to_group_ids(client, env)
        if not group_ids:
            return {
                "status": "error",
                "error": f"No groups found matching environment '{env}'",
            }

    # Fetch connectors
    all_connectors: List[Dict[str, Any]] = []
    if group_ids:
        for gid in group_ids:
            connectors = await _paginate(client, f"groups/{gid}/connectors")
            all_connectors.extend(connectors)
    else:
        all_connectors = await _paginate(client, "connectors")

    # Process and filter connectors
    results = []
    for c in all_connectors:
        connector_status = _get_connector_status(c)

        if status_filter != "all" and connector_status != status_filter:
            continue

        connector_id = c.get("id", "")
        status_info = c.get("status", {})
        warnings = status_info.get("warnings", [])

        results.append(
            {
                "id": connector_id,
                "service": c.get("service"),
                "schema": c.get("schema"),
                "group_id": c.get("group_id"),
                "connector_status": connector_status,
                "sync_state": status_info.get("sync_state"),
                "setup_state": status_info.get("setup_state"),
                "paused": c.get("paused", False),
                "warning_count": len(warnings),
                "dashboard_url": _get_connector_url(connector_id),
            }
        )

    filter_desc = []
    if env:
        filter_desc.append(f"env={env} ({', '.join(env_names)})")
    if status_filter != "all":
        filter_desc.append(f"status={status_filter}")

    logger.info(
        f"Listed {len(results)} connectors "
        f"(filters: {', '.join(filter_desc) if filter_desc else 'none'})"
    )

    return {
        "status": "success",
        "connectors": results,
        "count": len(results),
        "filters": {
            "env": env,
            "env_groups": env_names if env else None,
            "status": status_filter,
        },
    }


@mcp_tool
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
            - summary: Quick stats (total/enabled/disabled tables)
    """
    if error := validate_required_id(connector_id, "connector_id"):
        return error

    connector_id = connector_id.strip()
    client = get_fivetran_client()
    response = await client.get(f"connectors/{connector_id}/schemas")

    schemas_data = response.get("data", {}).get("schemas", {})

    schemas_list: List[Dict[str, Any]] = []
    total_tables = 0
    enabled_tables = 0

    for schema_name, schema_info in schemas_data.items():
        tables_list = []
        tables = schema_info.get("tables", {})

        for table_name, table_info in tables.items():
            is_enabled = table_info.get("enabled", False)
            total_tables += 1
            if is_enabled:
                enabled_tables += 1

            tables_list.append(
                {
                    "name": table_name,
                    "enabled": is_enabled,
                    "sync_mode": table_info.get("sync_mode"),
                }
            )

        schemas_list.append(
            {
                "name": schema_name,
                "enabled": schema_info.get("enabled", False),
                "tables": tables_list,
                "table_count": len(tables_list),
                "enabled_count": sum(1 for t in tables_list if t["enabled"]),
            }
        )

    logger.info(f"Retrieved schema status for connector: {connector_id}")

    return {
        "status": "success",
        "connector_id": connector_id,
        "dashboard_url": _get_connector_url(connector_id),
        "schemas": schemas_list,
        "schema_count": len(schemas_list),
        "summary": {
            "total_tables": total_tables,
            "enabled_tables": enabled_tables,
            "disabled_tables": total_tables - enabled_tables,
        },
    }


# Valid status filter values for hybrid agents
VALID_AGENT_STATUSES = {"all", "live", "offline"}


@mcp_tool
async def list_hybrid_agents(
    env: Optional[str] = None, status: Optional[str] = None
) -> Dict[str, Any]:
    """List Hybrid Deployment Agents, filtered by environment and/or status.

    Shows Local Processing Agents (hybrid agents) in your account,
    including their connection status and health.

    Args:
        env: Optional environment filter. Use human-readable names like "dev",
             "preprod", "prod", "sandbox" - matches against group names.
        status: Optional status filter. One of:
             - "all": All agents (default)
             - "live": Online/connected agents
             - "offline": Offline/disconnected agents

    Returns:
        Dict containing:
            - status: "success" or "error"
            - agents: List of agents with id, display_name, status info
            - count: Total number of agents
            - filters: The filters applied (env, status)

    Examples:
        list_hybrid_agents()                          # All agents
        list_hybrid_agents(env="prod")                # All prod agents
        list_hybrid_agents(status="offline")          # All offline agents
        list_hybrid_agents(env="prod", status="live") # Live prod agents
    """
    status_filter = (status or "all").lower()
    if status_filter not in VALID_AGENT_STATUSES:
        return {
            "status": "error",
            "error": f"Invalid status '{status}'. Must be one of: {', '.join(VALID_AGENT_STATUSES)}",
        }

    client = get_fivetran_client()

    # Resolve environment to group IDs
    group_ids: List[str] = []
    env_names: List[str] = []
    if env:
        group_ids, env_names = await _resolve_env_to_group_ids(client, env)
        if not group_ids:
            return {
                "status": "error",
                "error": f"No groups found matching environment '{env}'",
            }

    all_agents = await _paginate(client, "local-processing-agents")

    # Filter by env (no API calls needed)
    filtered_agents = []
    for agent in all_agents:
        agent_group_id = agent.get("group_id", "")
        if group_ids and agent_group_id not in group_ids:
            continue
        filtered_agents.append(agent)

    # Only fetch agent details if status filter requires it (avoids N+1 calls)
    need_status_check = status_filter != "all"

    results = []
    skipped_agents: List[str] = []
    for agent in filtered_agents:
        agent_id = agent.get("id", "")
        agent_group_id = agent.get("group_id", "")
        agent_status = "unknown"

        if need_status_check:
            try:
                detail_response = await client.get(
                    f"local-processing-agents/{agent_id}"
                )
                agent_detail = detail_response.get("data", {})
                is_online = agent_detail.get("online", False)
                agent_status = "live" if is_online else "offline"

                if agent_status != status_filter:
                    continue
            except FivetranAPIError:
                skipped_agents.append(agent_id)
                continue

        results.append(
            {
                "id": agent_id,
                "display_name": agent.get("display_name"),
                "group_id": agent_group_id,
                "agent_status": agent_status if need_status_check else "unknown",
                "registered_at": agent.get("registered_at"),
                "connector_count": len(agent.get("usage", [])),
            }
        )

    filter_desc = []
    if env:
        filter_desc.append(f"env={env} ({', '.join(env_names)})")
    if status_filter != "all":
        filter_desc.append(f"status={status_filter}")

    logger.info(
        f"Listed {len(results)} hybrid agents "
        f"(filters: {', '.join(filter_desc) if filter_desc else 'none'})"
    )

    response: Dict[str, Any] = {
        "status": "success",
        "agents": results,
        "count": len(results),
        "filters": {
            "env": env,
            "env_groups": env_names if env else None,
            "status": status_filter,
        },
    }

    if skipped_agents:
        response["warnings"] = {
            "skipped_agents": skipped_agents,
            "message": f"Could not fetch details for {len(skipped_agents)} agent(s)",
        }

    return response


@mcp_tool
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
            - agent_status: "live" or "offline"
            - registered_at: When the agent was registered
            - usage: List of connectors using this agent
    """
    if error := validate_required_id(agent_id, "agent_id"):
        return error

    agent_id = agent_id.strip()
    client = get_fivetran_client()
    response = await client.get(f"local-processing-agents/{agent_id}")

    agent = response.get("data", {})
    is_online = agent.get("online", False)

    logger.info(f"Retrieved details for hybrid agent: {agent_id}")

    return {
        "status": "success",
        "agent_id": agent_id,
        "display_name": agent.get("display_name"),
        "group_id": agent.get("group_id"),
        "agent_status": "live" if is_online else "offline",
        "registered_at": agent.get("registered_at"),
        "connector_count": len(agent.get("usage", [])),
        "usage": agent.get("usage", []),
    }


def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string to a timezone-aware datetime (UTC)."""
    if not ts:
        return None
    try:
        ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _hours_since(ts: Optional[str]) -> Optional[float]:
    """Calculate hours elapsed since a timestamp (compared to UTC now)."""
    dt = _parse_timestamp(ts)
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    delta = now - dt
    return round(delta.total_seconds() / 3600, 1)


@mcp_tool
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
    if error := validate_required_id(connector_id, "connector_id"):
        return error

    connector_id = connector_id.strip()
    client = get_fivetran_client()

    response = await client.get(f"connectors/{connector_id}")
    connector = response.get("data", {})
    status_info = connector.get("status", {})

    schema_response = await client.get(f"connectors/{connector_id}/schemas")
    schemas_data = schema_response.get("data", {}).get("schemas", {})

    sync_state = status_info.get("sync_state", "")
    setup_state = status_info.get("setup_state", "")
    paused = connector.get("paused", False)
    warnings = status_info.get("warnings", [])
    succeeded_at = connector.get("succeeded_at")
    failed_at = connector.get("failed_at")

    total_tables = 0
    enabled_tables = 0
    for schema_info in schemas_data.values():
        tables = schema_info.get("tables", {})
        for table_info in tables.values():
            total_tables += 1
            if table_info.get("enabled", False):
                enabled_tables += 1
    disabled_tables = total_tables - enabled_tables

    hours_since_success = _hours_since(succeeded_at)
    hours_since_failure = _hours_since(failed_at)

    issues: List[Dict[str, Any]] = []

    if paused:
        issues.append(
            {
                "severity": "info",
                "category": "status",
                "issue": "Connector is paused",
                "recommendation": "Resume the connector when ready to sync",
            }
        )

    if setup_state in ["broken", "incomplete"]:
        issues.append(
            {
                "severity": "high",
                "category": "setup",
                "issue": f"Setup is {setup_state}",
                "recommendation": "Complete connector setup in Fivetran dashboard",
            }
        )

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

    if warnings:
        issues.append(
            {
                "severity": "medium",
                "category": "warnings",
                "issue": f"{len(warnings)} active warning(s)",
                "details": warnings[:3],
                "recommendation": "Review and resolve warnings in Fivetran dashboard",
            }
        )

    if hours_since_failure and hours_since_failure < 24 and sync_state != "failed":
        issues.append(
            {
                "severity": "low",
                "category": "history",
                "issue": f"Had a failure {hours_since_failure} hours ago",
                "recommendation": "Monitor for recurring issues",
            }
        )

    if hours_since_success and hours_since_success > 48 and not paused:
        issues.append(
            {
                "severity": "medium",
                "category": "sync",
                "issue": f"No successful sync in {hours_since_success} hours",
                "recommendation": "Check if connector is stuck or having issues",
            }
        )

    if total_tables > 0 and disabled_tables > total_tables * 0.5:
        issues.append(
            {
                "severity": "low",
                "category": "schema",
                "issue": f"{disabled_tables} of {total_tables} tables disabled",
                "recommendation": "Verify table selection is intentional",
            }
        )

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


@mcp_tool
async def get_sync_history(
    connector_id: str, include_config: bool = False
) -> Dict[str, Any]:
    """Get sync history and recent events for a connector.

    Returns sync timestamps, recent warnings/errors with full details,
    and sync configuration. This extracts the maximum sync information
    available from the Fivetran REST API.

    Note: For detailed historical sync logs, set up the Fivetran Platform
    Connector which syncs logs to your destination.

    Args:
        connector_id: The unique identifier for the connector.
        include_config: Whether to include sync configuration details.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - connector_id: The connector ID
            - sync_state: Current sync state
            - last_syncs: Recent sync timestamps with calculated durations
            - warnings: Active warnings with full error details
            - sync_config: Sync frequency and schedule (if include_config=True)
    """
    if error := validate_required_id(connector_id, "connector_id"):
        return error

    connector_id = connector_id.strip()
    client = get_fivetran_client()
    response = await client.get(f"connectors/{connector_id}")
    connector = response.get("data", {})
    status_info = connector.get("status", {})

    succeeded_at = connector.get("succeeded_at")
    failed_at = connector.get("failed_at")
    sync_started = connector.get("sync_started")
    rescheduled_for = status_info.get("rescheduled_for")

    hours_since_success = _hours_since(succeeded_at)
    hours_since_failure = _hours_since(failed_at)

    sync_events: List[Dict[str, Any]] = []

    if succeeded_at:
        sync_events.append(
            {
                "type": "success",
                "timestamp": succeeded_at,
                "hours_ago": hours_since_success,
            }
        )

    if failed_at:
        sync_events.append(
            {
                "type": "failure",
                "timestamp": failed_at,
                "hours_ago": hours_since_failure,
            }
        )

    sync_events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

    warnings = status_info.get("warnings", [])
    warning_details: List[Dict[str, Any]] = [
        {
            "code": w.get("code", "unknown"),
            "message": w.get("message", ""),
            "details": w.get("details", {}),
        }
        for w in warnings
    ]

    tasks = status_info.get("tasks", [])
    task_details: List[Dict[str, Any]] = [
        {
            "code": t.get("code", "unknown"),
            "message": t.get("message", ""),
        }
        for t in tasks
    ]

    result: Dict[str, Any] = {
        "status": "success",
        "connector_id": connector_id,
        "dashboard_url": _get_connector_url(connector_id),
        "service": connector.get("service"),
        "schema": connector.get("schema"),
        "current_state": {
            "sync_state": status_info.get("sync_state"),
            "setup_state": status_info.get("setup_state"),
            "update_state": status_info.get("update_state"),
            "is_historical_sync": status_info.get("is_historical_sync", False),
            "paused": connector.get("paused", False),
        },
        "last_syncs": {
            "last_success": succeeded_at,
            "hours_since_success": hours_since_success,
            "last_failure": failed_at,
            "hours_since_failure": hours_since_failure,
            "sync_in_progress": sync_started is not None,
            "sync_started_at": sync_started,
            "rescheduled_for": rescheduled_for,
        },
        "recent_events": sync_events[:5],
        "warnings": {
            "count": len(warning_details),
            "details": warning_details,
        },
        "tasks": {
            "count": len(task_details),
            "details": task_details,
        },
    }

    if include_config:
        result["sync_config"] = {
            "sync_frequency": connector.get("sync_frequency"),
            "schedule_type": connector.get("schedule_type"),
            "daily_sync_time": connector.get("daily_sync_time"),
            "networking_method": connector.get("networking_method"),
            "local_processing_agent_id": connector.get("local_processing_agent_id"),
        }

    logger.info(f"Retrieved sync history for connector: {connector_id}")

    return result


@mcp_tool
async def get_group_health_summary(
    env: Optional[str] = None,
) -> Dict[str, Any]:
    """Get a health summary across all connectors in an environment/group.

    Provides a dashboard-style overview: connector counts by health status,
    the worst offenders, and time since the most recent failure. Useful for
    quickly triaging which environment needs attention.

    Args:
        env: Optional environment filter. Use human-readable names like "dev",
             "preprod", "prod", "sandbox" - matches against group names.
             Can also use exact group ID. If omitted, summarizes all connectors.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - summary: Counts by health status (healthy, failed, warning, paused)
            - worst_offenders: Top connectors with issues (up to 5)
            - groups: Which groups are included
            - total_connectors: Total connector count
    """
    client = get_fivetran_client()

    group_ids: List[str] = []
    env_names: List[str] = []
    if env:
        group_ids, env_names = await _resolve_env_to_group_ids(client, env)
        if not group_ids:
            return {
                "status": "error",
                "error": f"No groups found matching environment '{env}'",
            }

    all_connectors: List[Dict[str, Any]] = []
    if group_ids:
        for gid in group_ids:
            connectors = await _paginate(client, f"groups/{gid}/connectors")
            all_connectors.extend(connectors)
    else:
        all_connectors = await _paginate(client, "connectors")

    counts = {"healthy": 0, "failed": 0, "warning": 0, "paused": 0}
    worst_offenders: List[Dict[str, Any]] = []

    for c in all_connectors:
        connector_status = _get_connector_status(c)
        counts[connector_status] = counts.get(connector_status, 0) + 1

        if connector_status in ("failed", "warning"):
            status_info = c.get("status", {})
            connector_id = c.get("id", "")
            failed_at = c.get("failed_at")

            worst_offenders.append(
                {
                    "id": connector_id,
                    "service": c.get("service"),
                    "schema": c.get("schema"),
                    "group_id": c.get("group_id"),
                    "connector_status": connector_status,
                    "sync_state": status_info.get("sync_state"),
                    "setup_state": status_info.get("setup_state"),
                    "warning_count": len(status_info.get("warnings", [])),
                    "hours_since_failure": _hours_since(failed_at),
                    "dashboard_url": _get_connector_url(connector_id),
                }
            )

    # Sort worst offenders: failed first, then by recency of failure
    worst_offenders.sort(
        key=lambda x: (
            0 if x["connector_status"] == "failed" else 1,
            x.get("hours_since_failure") or 9999,
        )
    )

    logger.info(
        f"Generated health summary: {len(all_connectors)} connectors, "
        f"{counts['failed']} failed, {counts['warning']} warning"
    )

    return {
        "status": "success",
        "total_connectors": len(all_connectors),
        "health_counts": counts,
        "worst_offenders": worst_offenders[:5],
        "groups": {
            "env_filter": env,
            "group_names": env_names if env else None,
            "group_count": len(group_ids) if group_ids else None,
        },
    }


@mcp_tool
async def get_connector_config(connector_id: str) -> Dict[str, Any]:
    """Get the full configuration of a connector for troubleshooting.

    Shows networking, scheduling, source details, and hybrid agent assignment.
    Useful for diagnosing misconfiguration issues like wrong agent, incorrect
    networking method, or unexpected schedule changes.

    Args:
        connector_id: The unique identifier for the connector.

    Returns:
        Dict containing:
            - status: "success" or "error"
            - connector_id: The connector ID
            - service: Connector type (e.g. "postgres", "snowflake")
            - config: Source configuration details
            - networking: Networking method and agent assignment
            - schedule: Sync frequency and schedule type
            - destination: Group/destination info
    """
    if error := validate_required_id(connector_id, "connector_id"):
        return error

    connector_id = connector_id.strip()
    client = get_fivetran_client()
    response = await client.get(f"connectors/{connector_id}")
    connector = response.get("data", {})

    config = connector.get("config", {})
    status_info = connector.get("status", {})

    # Redact sensitive fields from config
    safe_config = {}
    sensitive_patterns = {
        "password",
        "secret",
        "token",
        "key",
        "credential",
        "auth_token",
        "connection_string",
        "certificate",
    }
    for key, value in config.items():
        if any(s in key.lower() for s in sensitive_patterns):
            safe_config[key] = "***REDACTED***"
        else:
            safe_config[key] = value

    logger.info(f"Retrieved config for connector: {connector_id}")

    return {
        "status": "success",
        "connector_id": connector_id,
        "dashboard_url": _get_connector_url(connector_id),
        "service": connector.get("service"),
        "service_version": connector.get("service_version"),
        "schema": connector.get("schema"),
        "networking": {
            "networking_method": connector.get("networking_method"),
            "local_processing_agent_id": connector.get("local_processing_agent_id"),
            "proxy_agent_id": connector.get("proxy_agent_id"),
        },
        "schedule": {
            "sync_frequency": connector.get("sync_frequency"),
            "schedule_type": connector.get("schedule_type"),
            "daily_sync_time": connector.get("daily_sync_time"),
            "paused": connector.get("paused", False),
        },
        "destination": {
            "group_id": connector.get("group_id"),
            "connected_by": connector.get("connected_by"),
            "created_at": connector.get("created_at"),
        },
        "current_state": {
            "sync_state": status_info.get("sync_state"),
            "setup_state": status_info.get("setup_state"),
        },
        "source_config": safe_config,
    }
