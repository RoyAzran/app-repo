"""Single FastMCP instance shared across all tool modules."""
import os
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http import TransportSecuritySettings

# Enable DNS rebinding protection in production; disable locally for dev
_dns_protection = os.environ.get("DISABLE_DNS_REBINDING_PROTECTION", "").lower() != "true"

# Allowed hostnames for DNS rebinding protection — must include the custom domain
_allowed_hosts = ["mcp-ads.com", "localhost", "127.0.0.1"]
_extra = os.environ.get("ALLOWED_HOSTS", "")
if _extra:
    _allowed_hosts.extend(h.strip() for h in _extra.split(",") if h.strip())

mcp = FastMCP(
    "Agency Tools",
    stateless_http=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=_dns_protection,
        allowed_hosts=_allowed_hosts,
    ),
)
