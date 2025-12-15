.PHONY: install clean test local container

# Dependency checks
deps:
	@which uv > /dev/null && echo "uv: $(shell uv --version)" || (echo "Error: uv not found. Please install uv." && exit 1)
	@which podman > /dev/null && echo "podman: $(shell podman --version)" || (echo "Error: podman not found. Please install podman." && exit 1)

# Install Python dependencies
install:
	@echo "Creating virtual environment..."
	@test -d .venv || uv venv
	@echo "Installing package with dev dependencies..."
	@. .venv/bin/activate && uv pip install -e ".[dev]"
	@echo "Installing pre-commit hooks..."
	@. .venv/bin/activate && pre-commit install
	@echo "Python dependencies installed successfully!"
	@echo "Activating virtual environment..."
	@echo '#!/bin/bash' > /tmp/activate_and_shell.sh
	@echo 'source .venv/bin/activate' >> /tmp/activate_and_shell.sh
	@echo 'echo "Virtual environment activated! Type exit to return to your original shell."' >> /tmp/activate_and_shell.sh
	@echo 'exec "$$SHELL"' >> /tmp/activate_and_shell.sh
	@chmod +x /tmp/activate_and_shell.sh
	@exec /tmp/activate_and_shell.sh

clean:
	rm -rf .mypy_cache .ruff_cache .venv __pycache__ activate_and_shell.sh

test:
	@if [ ! -d ".venv" ]; then \
		echo "Error: Virtual environment not found. Run 'make install' first to set up the environment."; \
		exit 1; \
	fi
	.venv/bin/python -m pytest

local:
	@echo "Setting up local environment..."
	@test -f .env || (echo "Creating .env from .env.example..." && cp .env.example .env)
	@echo "Starting MCP server locally on port 8080..."
	@echo "Health check available at: http://localhost:8080/health"
	@echo "Press Ctrl+C to stop the server"
	@. .venv/bin/activate && python -m fivetran_mcp_server.main

container:
	podman build -t fivetran-mcp-server -f Containerfile .
	podman run -p 8080:8080 --env-file .env fivetran-mcp-server
