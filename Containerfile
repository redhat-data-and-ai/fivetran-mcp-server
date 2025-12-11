FROM registry.access.redhat.com/ubi9/python-312:latest

# --------------------------------------------------------------------------------------------------
# set the working directory to /app
# --------------------------------------------------------------------------------------------------

WORKDIR /app

# --------------------------------------------------------------------------------------------------
# Install uv and create virtual environment
# --------------------------------------------------------------------------------------------------

USER root
RUN pip install uv && uv venv --python python3.12 /app/.venv

# --------------------------------------------------------------------------------------------------
# Copy project files and install dependencies
# --------------------------------------------------------------------------------------------------

COPY pyproject.toml uv.lock README.md /app/
COPY fivetran_mcp_server /app/fivetran_mcp_server

# Install dependencies into the venv
RUN uv pip install --python /app/.venv/bin/python .

USER default

# --------------------------------------------------------------------------------------------------
# Set PYTHONPATH to include /app
# --------------------------------------------------------------------------------------------------

ENV PYTHONPATH=/app

# --------------------------------------------------------------------------------------------------
# add entrypoint for the container
# --------------------------------------------------------------------------------------------------

CMD ["/app/.venv/bin/python", "-m", "fivetran_mcp_server.main"]
