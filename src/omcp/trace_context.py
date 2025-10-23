"""
Trace context sharing between fastomop and OMCP subprocess.
Uses a temporary file to receive dynamic trace context from parent process.

Langfuse V3 Note:
- V3 uses W3C Trace Context standard via OpenTelemetry
- Uses traceparent header for proper context propagation
- Child spans automatically nest within the parent trace
"""

import os
import json
import platform
import tempfile
from pathlib import Path
from typing import Optional, Dict


# Shared trace context file path (same as fastomop)
# Use platform-specific temp directory for cross-platform compatibility
if platform.system() == "Windows":
    default_path = Path(tempfile.gettempdir()) / ".fastomop_langfuse_trace_context.json"
else:
    # On Unix-like systems (macOS/Linux), use /tmp for consistency across processes
    # tempfile.gettempdir() can return different paths in different contexts on macOS
    default_path = Path("/tmp") / ".fastomop_langfuse_trace_context.json"

TRACE_CONTEXT_FILE = Path(
    os.environ.get("LANGFUSE_TRACE_CONTEXT_FILE", str(default_path))
)


def read_trace_context() -> Dict[str, Optional[str]]:
    """
    Read current trace context from shared file.

    Returns W3C Trace Context headers for proper OpenTelemetry propagation.

    Returns:
        Dict with traceparent, tracestate, and session_id (or None if not available)
        traceparent format: version-trace_id-parent_span_id-trace_flags
        Example: "00-xxxxxx-b7ad6b7169203331-01"
    """
    try:
        if TRACE_CONTEXT_FILE.exists():
            with open(TRACE_CONTEXT_FILE, "r") as f:
                context = json.load(f)
                return {
                    "traceparent": context.get("traceparent"),
                    "tracestate": context.get("tracestate"),
                    "session_id": context.get("session_id"),
                    # Backward compatibility: also read old trace_id format
                    "trace_id": context.get("trace_id"),
                }
    except Exception as e:
        # Non-critical error, return empty context
        # Only log if file exists but can't be read (actual error)
        if TRACE_CONTEXT_FILE.exists():
            import logging

            logger = logging.getLogger("omcp")
            logger.warning(
                f"Failed to read trace context from {TRACE_CONTEXT_FILE}: {e}"
            )

    return {
        "traceparent": None,
        "tracestate": None,
        "session_id": None,
        "trace_id": None,
    }
