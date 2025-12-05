# Fivetran MCP Server Core

Core implementation of the Fivetran MCP server.

## Directory Structure

```
src/
├── main.py              # Server entry point
├── api.py               # FastAPI application
├── mcp.py               # MCP server & tool registration
├── settings.py          # Configuration
├── fivetran_client.py   # Fivetran API client
└── tools/
    └── connectors.py    # Connector troubleshooting tools
```

## Key Files

- **`fivetran_client.py`** - HTTP client for Fivetran REST API with Basic Auth
- **`tools/connectors.py`** - All connector-related tools with environment filtering
- **`mcp.py`** - Registers tools with FastMCP
