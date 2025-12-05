# Tests

Test suite for the Fivetran MCP server.

## Running Tests

```bash
# All tests
pytest

# With verbose output
pytest -v

# Specific file
pytest tests/test_mcp.py -v
```

## Test Files

| File | Description |
|------|-------------|
| `conftest.py` | Pytest fixtures |
| `test_api.py` | FastAPI endpoint tests |
| `test_basic.py` | Basic functionality tests |
| `test_main.py` | Server startup tests |
| `test_mcp.py` | MCP server tests |
| `test_settings.py` | Configuration tests |
| `test_utils.py` | Utility function tests |
