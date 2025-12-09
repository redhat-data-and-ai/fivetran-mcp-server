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
| `list_groups()` | List all Fivetran groups/destinations |
| `list_connectors(group_id?)` | List connectors, optionally filtered by group |
| `list_failed_connectors(group_id?)` | Find connectors with issues |
| `get_connector_schema_status(connector_id)` | Get table-level sync status |
| `diagnose_connector(connector_id)` | **Smart** health check with recommendations |
| `list_hybrid_agents()` | List all Hybrid Deployment Agents and their status |
| `get_hybrid_agent_details(agent_id)` | Get details for a specific hybrid agent |

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
ENABLE_AUTH=false \
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
| `ENABLE_AUTH` | `true` | Enable OAuth (set to `false` for local dev) |
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

### List connectors in a specific group
```
list_connectors(group_id="abc123")
```

### Find failed connectors
```
list_failed_connectors()
```

### Find failed connectors in a specific group
```
list_failed_connectors(group_id="abc123")
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
```

### Get hybrid agent details
```
get_hybrid_agent_details(agent_id="abc123")
```

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
│   ├── oauth/               # OAuth 2.0 (when ENABLE_AUTH=true)
│   ├── storage/             # PostgreSQL storage
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
