# Fivetran MCP Server

A read-only MCP (Model Context Protocol) server for troubleshooting and diagnosing Fivetran connector issues.

## Features

- **Troubleshooting-focused**: Quickly identify failed or problematic connectors
- **Read-only**: Safe operations only - no modifications to your Fivetran setup
- **Cursor-compatible**: Works with Cursor IDE's MCP integration
- **Generic**: Works with any Fivetran account structure

## Available Tools

| Tool | Description |
|------|-------------|
| `list_connectors(env?, status?)` | List connectors filtered by environment and/or status |
| `list_hybrid_agents(env?, status?)` | List hybrid agents filtered by environment and/or status |
| `get_connector_schema_status(connector_id)` | Get table-level sync status |
| `diagnose_connector(connector_id)` | **Smart** health check with recommendations |
| `get_sync_history(connector_id, include_config?)` | Get sync timestamps, warnings with error details, and config |
| `get_hybrid_agent_details(agent_id)` | Get details for a specific hybrid agent |

### list_connectors Parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `env` | `"dev"`, `"preprod"`, `"prod"`, `"sandbox"`, etc. | Filter by environment (partial match on group name) |
| `status` | `"all"`, `"failed"`, `"healthy"`, `"paused"`, `"warning"` | Filter by connector health |

### list_hybrid_agents Parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `env` | `"dev"`, `"preprod"`, `"prod"`, `"sandbox"`, etc. | Filter by environment (partial match on group name) |
| `status` | `"all"`, `"live"`, `"offline"` | Filter by agent connection status |

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Fivetran API credentials ([Get them here](https://fivetran.com/docs/rest-api/getting-started))

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd fivetran-mcp-server

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your Fivetran API credentials
```

### Running the Server

```bash
# Option 1: Using environment variables directly
FIVETRAN_API_KEY="your-key" \
FIVETRAN_API_SECRET="your-secret" \
uv run python -m fivetran_mcp_server.main

# Option 2: Using .env file (after configuring it)
uv run python -m fivetran_mcp_server.main
```

The server starts at `http://localhost:8080`

### Verify It's Working

```bash
curl http://localhost:8080/health
```

## Using with Cursor

1. Create `.cursor/mcp.json` in your project:

```json
{
  "mcpServers": {
    "fivetran": {
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

2. Start the server (see above)

3. Reload Cursor (`Cmd+Shift+P` → "Developer: Reload Window")

4. Ask questions like:
   - "List my Fivetran groups"
   - "Show connectors in group X"
   - "Show failed connectors"
   - "Get details for connector xyz"
   - "Show me all hybrid agents"
   - "What's the status of hybrid agent abc?"

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `FIVETRAN_API_KEY` | (required) | Your Fivetran API key |
| `FIVETRAN_API_SECRET` | (required) | Your Fivetran API secret |
| `FIVETRAN_BASE_URL` | `https://api.fivetran.com/v1` | Fivetran API base URL |
| `MCP_HOST` | `localhost` | Server bind address |
| `MCP_PORT` | `8080` | Server port |
| `PYTHON_LOG_LEVEL` | `INFO` | Logging level |

## Example Usage

### List all groups
```
list_groups()
```

### List all connectors
```
list_connectors()
```

### List connectors by environment
```
list_connectors(env="prod")
list_connectors(env="preprod")
list_connectors(env="dev")
```

### List failed connectors
```
list_connectors(status="failed")
```

### List failed connectors in a specific environment
```
list_connectors(env="prod", status="failed")
```

### Diagnose a connector (health check with recommendations)
```
diagnose_connector(connector_id="abc123")
```

### Check table sync status
```
get_connector_schema_status(connector_id="abc123")
```

### List hybrid deployment agents
```
list_hybrid_agents()
list_hybrid_agents(env="prod")
list_hybrid_agents(status="offline")
list_hybrid_agents(env="prod", status="live")
```

### Get hybrid agent details
```
get_hybrid_agent_details(agent_id="abc123")
```

### Get sync history and warnings
```
get_sync_history(connector_id="abc123")
get_sync_history(connector_id="abc123", include_config=True)
```

Returns sync timestamps (last success/failure), active warnings with full error details, and optionally sync configuration. Warnings contain the actual error messages that explain why a connector failed.

### Diagnose a connector (smart health check)
```
diagnose_connector(connector_id="abc123")
```

Returns overall health status, issues with severity levels, and actionable recommendations.

## Project Structure

```
fivetran-mcp-server/
├── fivetran_mcp_server/
│   ├── main.py              # Entry point
│   ├── api.py               # FastAPI app
│   ├── mcp.py               # MCP server & tool registration
│   ├── settings.py          # Configuration
│   ├── fivetran_client.py   # Fivetran API client
│   ├── tools/
│   │   └── connectors.py    # Connector tools
│   └── utils/
│       └── pylogger.py      # Logging
├── tests/                   # Test suite
├── .env.example             # Environment template
├── pyproject.toml           # Dependencies
└── README.md
```

## License

Apache 2.0
