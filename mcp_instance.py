"""Single FastMCP instance shared across all tool modules."""
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http import TransportSecuritySettings

mcp = FastMCP(
    "Agency Tools",
    stateless_http=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)
