"""
Tool registration module.
Importing this file causes all tool modules to register their @mcp.tool() handlers
onto the shared mcp singleton from mcp_instance.py.
Keep imports in this file only — do not import mcp_server from tool files.
"""
import tools.google_ads   # noqa: F401
import tools.meta_ads     # noqa: F401
import tools.ga4          # noqa: F401
import tools.gsc          # noqa: F401
import tools.sheets       # noqa: F401
