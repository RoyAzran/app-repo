"""Single FastMCP instance shared across all tool modules."""
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Agency Tools", stateless_http=True, json_response=True)
