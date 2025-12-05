# Fivetran MCP Tools

All Fivetran troubleshooting tools are implemented here.

## Current Tools

| Tool | File | Description |
|------|------|-------------|
| `list_groups` | `connectors.py` | List all groups/destinations |
| `list_connectors` | `connectors.py` | List connectors (filter by group_id) |
| `get_connector_details` | `connectors.py` | Get full connector details |
| `list_failed_connectors` | `connectors.py` | Find problem connectors |
| `get_connector_schema_status` | `connectors.py` | Table-level sync status |
| `list_hybrid_agents` | `connectors.py` | List all hybrid deployment agents |
| `get_hybrid_agent_details` | `connectors.py` | Get hybrid agent details |

## Adding New Tools

1. Add your function to an existing file or create a new one
2. Register in `../mcp.py`
3. Follow this pattern:

```python
async def your_tool(param: str) -> Dict[str, Any]:
    """Tool description.
    
    Args:
        param: Parameter description.
        
    Returns:
        Dict with status and results.
    """
    try:
        client = get_fivetran_client()
        # ... your logic
        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "error": str(e)}
```
