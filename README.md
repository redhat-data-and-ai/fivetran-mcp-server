# Fivetran MCP Server

A read-only MCP (Model Context Protocol) server for troubleshooting Fivetran connectors. Connect it to Cursor (or any MCP client) and diagnose sync issues, check connector health, and inspect configurations using natural language.

## What It Does

- Lists and filters connectors by environment and health status
- Diagnoses connector issues with actionable recommendations
- Shows environment-level health summaries at a glance
- Inspects connector configuration (with sensitive fields redacted)
- Monitors hybrid deployment agents

**Read-only** — no modifications to your Fivetran setup, ever.

## Available Tools

| Tool | What it does |
|------|--------------|
| `list_connectors` | List connectors, filter by env and status |
| `get_group_health_summary` | Dashboard-style overview of an environment |
| `diagnose_connector` | Health check with severity-ranked issues and recommendations |
| `get_connector_config` | Full connector configuration (credentials redacted) |
| `get_connector_schema_status` | Table-level sync status |
| `get_sync_history` | Sync timestamps, warnings, and error details |
| `list_hybrid_agents` | List hybrid deployment agents |
| `get_hybrid_agent_details` | Detailed agent status and assigned connectors |

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Fivetran API credentials ([Get them here](https://fivetran.com/docs/rest-api/getting-started))

### 1. Install

```bash
git clone https://github.com/redhat-data-and-ai/fivetran-mcp-server.git
cd fivetran-mcp-server
make install
```

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env` and add your Fivetran API key and secret:

```
FIVETRAN_API_KEY=your-api-key
FIVETRAN_API_SECRET=your-api-secret
```

### 3. Run

```bash
make local
```

The server starts at `http://localhost:8080`. Verify with:

```bash
curl http://localhost:8080/health
```

### 4. Connect to Cursor

Add to `.cursor/mcp.json` in your project:

```json
{
  "mcpServers": {
    "fivetran": {
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

Reload Cursor (`Cmd+Shift+P` → "Developer: Reload Window"), then ask:

- "Show me failed connectors in prod"
- "Diagnose connector shouldn_snack"
- "Give me a health summary for dev"
- "What's the config for connector abc123?"
- "List offline hybrid agents"

## Tool Reference

### list_connectors

List connectors filtered by environment and/or health status.

```
list_connectors(env="prod", status="failed")
```

| Parameter | Values | Description |
|-----------|--------|-------------|
| `env` | `"dev"`, `"preprod"`, `"prod"`, `"sandbox"`, or group ID | Partial match on group name |
| `status` | `"all"`, `"failed"`, `"healthy"`, `"paused"`, `"warning"` | Filter by health |

### get_group_health_summary

Get an environment-level dashboard: connector counts by status plus the top 5 worst offenders.

```
get_group_health_summary(env="prod")
```

### diagnose_connector

Comprehensive health check with severity-ranked issues and actionable recommendations.

```
diagnose_connector(connector_id="abc123")
```

### get_connector_config

Inspect full connector configuration (networking, schedule, source settings). Sensitive fields like passwords, tokens, and keys are automatically redacted.

```
get_connector_config(connector_id="abc123")
```

### get_connector_schema_status

See which tables are enabled/disabled and their sync modes.

```
get_connector_schema_status(connector_id="abc123")
```

### get_sync_history

Get sync timestamps, active warnings with error messages, and optionally the sync schedule config.

```
get_sync_history(connector_id="abc123")
get_sync_history(connector_id="abc123", include_config=True)
```

### list_hybrid_agents

List hybrid deployment agents filtered by environment and/or connection status.

```
list_hybrid_agents(env="prod", status="live")
```

| Parameter | Values | Description |
|-----------|--------|-------------|
| `env` | `"dev"`, `"preprod"`, `"prod"`, `"sandbox"` | Partial match on group name |
| `status` | `"all"`, `"live"`, `"offline"` | Filter by connection status |

### get_hybrid_agent_details

Get detailed info for a specific agent including assigned connectors.

```
get_hybrid_agent_details(agent_id="abc123")
```

## Docker

```bash
# Pre-built image
docker pull ghcr.io/redhat-data-and-ai/fivetran-mcp-server:latest

docker run -p 8080:8080 \
  -e FIVETRAN_API_KEY=your_key \
  -e FIVETRAN_API_SECRET=your_secret \
  ghcr.io/redhat-data-and-ai/fivetran-mcp-server:latest
```

Or build your own:

```bash
docker build -t fivetran-mcp-server -f Containerfile .
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FIVETRAN_API_KEY` | (required) | Fivetran API key |
| `FIVETRAN_API_SECRET` | (required) | Fivetran API secret |
| `FIVETRAN_BASE_URL` | `https://api.fivetran.com/v1` | API base URL |
| `MCP_HOST` | `localhost` | Server bind address |
| `MCP_PORT` | `8080` | Server port |
| `MCP_TRANSPORT_PROTOCOL` | `http` | Transport: `http`, `streamable-http`, or `sse` |
| `PYTHON_LOG_LEVEL` | `INFO` | Log level |

## Development

```bash
make install          # Set up venv + deps + pre-commit hooks
make test             # Run tests
make local            # Start the server locally
```

### Running Tests

```bash
.venv/bin/python -m pytest                              # All tests
.venv/bin/python -m pytest --cov=fivetran_mcp_server    # With coverage
.venv/bin/python -m pytest tests/test_connectors.py -v  # Specific file
```

### Code Quality

```bash
ruff check .    # Lint
ruff format .   # Format
```

## Project Structure

```
fivetran-mcp-server/
├── fivetran_mcp_server/
│   ├── main.py              # Entry point (uvicorn)
│   ├── api.py               # FastAPI app + health endpoint
│   ├── mcp.py               # MCP server + tool registration
│   ├── settings.py          # Pydantic settings + validation
│   ├── fivetran_client.py   # HTTP client (retries, timeouts, pooling)
│   ├── tools/
│   │   ├── __init__.py      # Shared error handling decorator
│   │   └── connectors.py    # All MCP tool implementations
│   └── utils/
│       └── pylogger.py      # Structured logging (structlog)
├── tests/                   # Pytest suite (138 tests, 80%+ coverage)
├── .env.example             # Environment template
├── Containerfile            # OCI container build
├── Makefile                 # Dev commands
└── pyproject.toml           # Dependencies + tool config
```

## License

Apache 2.0
