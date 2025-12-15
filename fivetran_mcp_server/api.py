"""This module sets up the FastAPI application for the Fivetran MCP server.

It initializes the FastAPI app, configures CORS middleware, and sets up
the MCP server with appropriate transport protocols.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from fivetran_mcp_server.mcp import FivetranMCPServer
from fivetran_mcp_server.settings import settings
from fivetran_mcp_server.utils.pylogger import get_python_logger

logger = get_python_logger(settings.PYTHON_LOG_LEVEL)

server = FivetranMCPServer()

# Choose the appropriate transport protocol based on settings
if settings.MCP_TRANSPORT_PROTOCOL == "sse":
    from fastmcp.server.http import create_sse_app

    mcp_app = create_sse_app(server.mcp, message_path="/sse/message", sse_path="/sse")
else:  # Default to standard HTTP (works for both "http" and "streamable-http")
    mcp_app = server.mcp.http_app(path="/mcp")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan handler for MCP initialization."""
    # Run MCP lifespan
    async with mcp_app.lifespan(app):
        logger.info("Server is ready to accept connections")
        yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    """Health check endpoint for the MCP server."""
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "service": "fivetran-mcp-server",
            "transport_protocol": settings.MCP_TRANSPORT_PROTOCOL,
            "version": "0.1.0",
        },
    )


app.mount("/", mcp_app)

if settings.CORS_ENABLED:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=settings.CORS_CREDENTIALS,
        allow_methods=settings.CORS_METHODS,
        allow_headers=settings.CORS_HEADERS,
    )
