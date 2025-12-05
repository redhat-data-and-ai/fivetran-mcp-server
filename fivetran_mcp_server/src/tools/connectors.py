"""Fivetran connector tools for troubleshooting and diagnostics (read-only).

This module provides read-only MCP tools for diagnosing Fivetran connector issues.
Works with any Fivetran account - filter connectors by group_id.
"""

from typing import Any, Dict, List, Optional

from fivetran_mcp_server.src.fivetran_client import get_fivetran_client
from fivetran_mcp_server.utils.pylogger import get_python_logger

logger = get_python_logger()


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
        response = await client.get("groups")

        groups = response.get("data", {}).get("items", [])

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
            # Fetch from specific group
            response = await client.get(f"groups/{group_id}/connectors")
            all_connectors = response.get("data", {}).get("items", [])
            filter_desc = f"group_id={group_id}"
        else:
            # Fetch all connectors
            response = await client.get("connectors")
            all_connectors = response.get("data", {}).get("items", [])
            filter_desc = "none"

        # Simplify connector info
        simplified = [
            {
                "id": c.get("id"),
                "service": c.get("service"),
                "schema": c.get("schema"),
                "group_id": c.get("group_id"),
                "paused": c.get("paused", False),
            }
            for c in all_connectors
        ]

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


async def get_connector_details(connector_id: str) -> Dict[str, Any]:
    """Get comprehensive details for a connector (for troubleshooting).

    Retrieves full connector information including configuration,
    sync status, last sync times, and any warnings or failures.

    Args:
        connector_id: The unique identifier for the connector.

    Returns:
        Dict containing connector details including service, schema,
        sync state, succeeded_at, failed_at, warnings, etc.
    """
    try:
        client = get_fivetran_client()
        response = await client.get(f"connectors/{connector_id}")

        c = response.get("data", {})
        status_info = c.get("status", {})

        logger.info(f"Retrieved details for connector: {connector_id}")

        return {
            "status": "success",
            "connector_id": connector_id,
            "service": c.get("service"),
            "schema": c.get("schema"),
            "group_id": c.get("group_id"),
            "paused": c.get("paused", False),
            "sync_state": status_info.get("sync_state"),
            "setup_state": status_info.get("setup_state"),
            "succeeded_at": c.get("succeeded_at"),
            "failed_at": c.get("failed_at"),
            "sync_frequency": c.get("sync_frequency"),
            "schedule_type": c.get("schedule_type"),
            "warnings": status_info.get("warnings", []),
            "tasks": status_info.get("tasks", []),
            "failure_reason": c.get("failure_reason"),
        }

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error getting connector {connector_id}: {e}")
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

        # Fetch connectors
        if group_id:
            response = await client.get(f"groups/{group_id}/connectors")
            all_connectors = response.get("data", {}).get("items", [])
        else:
            response = await client.get("connectors")
            all_connectors = response.get("data", {}).get("items", [])

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
                failed.append({
                    "id": c.get("id"),
                    "service": c.get("service"),
                    "schema": c.get("schema"),
                    "group_id": c.get("group_id"),
                    "sync_state": sync_state,
                    "setup_state": setup_state,
                    "warning_count": len(warnings),
                    "warnings": warnings[:3],
                })

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
                tables_list.append({
                    "name": table_name,
                    "enabled": table_info.get("enabled", False),
                    "sync_mode": table_info.get("sync_mode"),
                })

            schemas_list.append({
                "name": schema_name,
                "enabled": schema_info.get("enabled", False),
                "tables": tables_list,
                "table_count": len(tables_list),
            })

        logger.info(f"Retrieved schema status for connector: {connector_id}")

        return {
            "status": "success",
            "connector_id": connector_id,
            "schemas": schemas_list,
            "schema_count": len(schemas_list),
        }

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Error getting schema status for {connector_id}: {e}")
        return {"status": "error", "error": str(e)}
