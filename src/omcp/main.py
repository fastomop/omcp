import os
import sys
import signal
from mcp.server.fastmcp import FastMCP
import mcp
from dotenv import load_dotenv, find_dotenv
from omcp.config import langfuse, logger
from omcp.trace_context import read_trace_context

import uuid
import time
import traceback
from functools import wraps

# OpenTelemetry context propagation
from opentelemetry.propagate import extract
from opentelemetry import context as otel_context_api


# --- Per-tool decorator to capture context + Langfuse trace ---
def capture_context(tool_name=None):
    """
    Decorator to capture the incoming MCP tool call:
      - tries to extract common context fields (messages, prompt, input, payload)
      - attempts to capture any LLM prompt context if available
      - starts a Langfuse per-request trace/span/generation (if enabled)
      - records input/output to Langfuse (if enabled)
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            request_id = str(uuid.uuid4())
            ts = time.strftime("%d/%m/%y %H:%M:%S", time.localtime())

            extracted = {}

            # Common names that may hold context in MCP payloads
            possible_keys = (
                "payload",
                "messages",
                "prompt",
                "input",
                "tool_input",
                "data",
                "query",
                "conversation",
                "context",
                "user_message",
                "system_message",
                "chat_history",
                "request_context",
                "llm_context",
                "conversation_history",
            )

            # 1) Extract from kwargs
            for k in possible_keys:
                if k in kwargs:
                    extracted[k] = kwargs[k]

            # 2) Extract from args
            if args:
                for i, arg in enumerate(args):
                    arg_key = f"arg_{i}"
                    extracted[arg_key] = arg

                    # If the argument is a dict, look for prompt-related keys
                    if isinstance(arg, dict):
                        for prompt_key in [
                            "prompt",
                            "messages",
                            "conversation",
                            "context",
                        ]:
                            if prompt_key in arg:
                                extracted[f"nested_{prompt_key}"] = arg[prompt_key]

            # 3) Try to extract from the current execution context
            import inspect

            frame = inspect.currentframe()
            try:
                # Look for any variables in calling frames that might contain prompt info
                caller_frame = frame.f_back.f_back if frame and frame.f_back else None
                if caller_frame:
                    caller_locals = caller_frame.f_locals
                    for var_name in ["prompt", "messages", "conversation", "request"]:
                        if var_name in caller_locals:
                            extracted[f"caller_{var_name}"] = str(
                                caller_locals[var_name]
                            )[:1000]  # Truncate for safety
            except Exception:
                pass  # Ignore frame inspection errors
            finally:
                del frame  # Prevent reference cycles

            # 4) Capture environment variables that might contain relevant context
            env_context = {}
            env_keys = [
                "MCP_CLIENT_INFO",
                "CONVERSATION_ID",
                "SESSION_ID",
                "USER_CONTEXT",
            ]
            for env_key in env_keys:
                if os.environ.get(env_key):
                    env_context[env_key] = os.environ.get(env_key)

            if env_context:
                extracted["environment_context"] = env_context

            # Enhanced call metadata
            call_meta = {
                "tool": tool_name or func.__name__,
                "request_id": request_id,
                "timestamp": ts,
                "func_name": func.__name__,
                "func_args_count": len(args),
                # Truncated representations for logging
                "func_args_repr": repr(args)[:1000] if args else "[]",
            }

            # Start Langfuse logging for this single call (if enabled)
            if langfuse:
                try:
                    # Read trace context from shared file (propagated from fastomop)
                    trace_ctx = read_trace_context()
                    traceparent = trace_ctx.get("traceparent")

                    # Prepare comprehensive input data
                    input_data = {
                        "extracted_context": extracted,
                        "call_metadata": call_meta,
                        "raw_args": [
                            str(arg)[:500] for arg in args
                        ],  # Truncated string representations
                    }

                    # Add prompt-specific metadata if found
                    prompt_metadata = {}
                    for key, value in extracted.items():
                        if any(
                            prompt_word in key.lower()
                            for prompt_word in ["prompt", "message", "conversation"]
                        ):
                            prompt_metadata[key] = value

                    if prompt_metadata:
                        input_data["prompt_related_data"] = prompt_metadata
                        # Log specifically that we found prompt-related content
                        logger.info(
                            f"Captured prompt-related metadata for {call_meta['tool']}: {list(prompt_metadata.keys())}"
                        )

                    # Extract OpenTelemetry context from W3C Trace Context headers
                    # This properly propagates parent-child relationships across processes
                    context_token = None
                    if traceparent:
                        # Build carrier dict with W3C headers
                        carrier = {"traceparent": traceparent}
                        if trace_ctx.get("tracestate"):
                            carrier["tracestate"] = trace_ctx["tracestate"]

                        # Extract context using OpenTelemetry propagator
                        extracted_context = extract(carrier)

                        # Attach the extracted context to make it current
                        # This allows Langfuse to automatically use it for span creation
                        context_token = otel_context_api.attach(extracted_context)

                        logger.info(
                            f"Linking {call_meta['tool']} to parent trace (OpenTelemetry context)"
                        )
                    else:
                        # No parent context, create standalone span
                        logger.debug(
                            f"No parent trace context, creating standalone span for {call_meta['tool']}"
                        )

                    try:
                        # Use context manager for span
                        # Note: Using start_as_current_span for DB operations (not LLM calls)
                        # The span will automatically use the attached context
                        with langfuse.start_as_current_span(
                            name=call_meta["tool"]
                        ) as span:
                            # Update with input data
                            span.update(input=input_data)

                            try:
                                response = func(*args, **kwargs)
                                # Update with output
                                span.update(
                                    output={
                                        "response": response,
                                        "response_type": type(response).__name__,
                                    }
                                )
                                return response

                            except Exception as ex:
                                err_info = {
                                    "error": str(ex),
                                    "error_type": type(ex).__name__,
                                    "traceback": traceback.format_exc(),
                                }
                                span.update(output=err_info)
                                logger.error(
                                    f"Tool {call_meta['tool']} failed: {str(ex)}"
                                )
                                raise
                    finally:
                        # Detach the context to restore the previous context
                        if context_token is not None:
                            otel_context_api.detach(context_token)

                except Exception as langfuse_error:
                    # If Langfuse fails for any reason, don't crash the server
                    logger.exception(
                        "Langfuse logging failed for tool %s (request %s): %s",
                        call_meta["tool"],
                        request_id,
                        str(langfuse_error),
                    )
                    # Fall through to execute function without Langfuse logging

            # Execute function without Langfuse logging (if disabled or failed)
            try:
                response = func(*args, **kwargs)
                return response
            except Exception as func_error:
                logger.error(f"Function execution failed: {str(func_error)}")
                raise

        return wrapper

    return decorator


# Import with fallback strategy
try:
    from omcp.db import OmopDatabase

    logger.info("Using enhanced OmopDatabase with robust connection handling")
except ImportError:
    logger.error("Failed to import OmopDatabase")
    sys.exit(1)

load_dotenv(find_dotenv())

# Global variable for graceful shutdown
db = None


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    if db and hasattr(db, "_conn") and db._conn:
        try:
            db._conn.disconnect()
            logger.info("Database connection closed")
        except Exception as shutdown_error:
            logger.warning(
                f"Error closing database connection during shutdown: {shutdown_error}"
            )
    sys.exit(0)


# Set up signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# Get database configuration
db_type = os.environ.get("DB_TYPE")
db_path = os.environ.get("DB_PATH")
db_read_only = os.environ.get("DB_READ_ONLY", "false").lower() == "true"

if db_type == "duckdb":
    if db_read_only:
        # Use read-only mode for DuckDB to prevent file locking issues
        connection_string = f"duckdb:///{db_path}?access_mode=read_only"
        logger.info("Using DuckDB in read-only mode to prevent file locking")
    else:
        connection_string = f"duckdb:///{db_path}"
elif db_type == "postgres":
    connection_string = (
        f"postgresql://{os.environ.get('DB_USERNAME')}:"
        f"{os.environ.get('DB_PASSWORD')}@{os.environ.get('DB_HOST')}:"
        f"{os.environ.get('DB_PORT')}/{os.environ.get('DB_DATABASE')}"
    )
elif db_type == "databricks":
    # Databricks connection string format
    db_token = os.environ.get("DB_TOKEN")
    db_host = os.environ.get("DB_HOST")
    db_http_path = os.environ.get("DB_HTTP_PATH")
    db_catalog = os.environ.get("DB_CATALOG", "hive_metastore")
    db_schema = os.environ.get("DB_SCHEMA", "default")

    # URL encode the parameters properly
    from urllib.parse import quote_plus

    connection_string = (
        f"databricks://?server_hostname={quote_plus(db_host)}"
        f"&http_path={quote_plus(db_http_path)}"
        f"&access_token={quote_plus(db_token)}"
        f"&catalog={quote_plus(db_catalog)}"
        f"&schema={quote_plus(db_schema)}"
    )
    logger.info(f"Using Databricks with catalog={db_catalog}, schema={db_schema}")
else:
    raise ValueError(
        "Unsupported DB_TYPE. Must be 'duckdb', 'postgres', or 'databricks'."
    )

logger.info(f"Initializing OMCP server with {db_type} database...")

# MCP Server configuration
transport_type = os.environ.get("MCP_TRANSPORT", "stdio").lower()
host = os.environ.get("MCP_HOST", "localhost")
port = int(os.environ.get("MCP_PORT", "8080"))

# Validate transport type
if transport_type not in ["stdio", "sse"]:
    logger.error(f"Invalid transport type: {transport_type}. Must be 'stdio' or 'sse'.")
    sys.exit(1)

logger.info(f"Using {transport_type.upper()} transport")

# Initialize FastMCP
mcp_app = FastMCP(name="OMOP MCP Server")

# Initialize database with robust error handling
try:
    db = OmopDatabase(
        connection_string=connection_string,
        cdm_schema=os.environ.get("CDM_SCHEMA", "base"),
        vocab_schema=os.environ.get("VOCAB_SCHEMA", "base"),
        read_only=db_read_only,
    )
    logger.info(f"Database initialized successfully (read-only: {db_read_only})")
except Exception as e:
    logger.error(f"Failed to initialize database: {e}")
    sys.exit(1)


@mcp_app.tool(
    name="Get_Information_Schema",
    description="Get the information schema of the OMOP database.",
)
@capture_context(tool_name="Get_Information_Schema")
def get_information_schema() -> mcp.types.CallToolResult:
    """Get the information schema of the OMOP database.

    This function retrieves information from the information schema of the OMOP database.
    Information is restricted to only tables and columns allowed by the users configuration.
    Args:
        None
    Returns:
        List of schemas, tables, columns and data types formatted as a CSV string.
    """
    try:
        logger.debug("Getting information schema...")
        # Note: @capture_context decorator already handles Langfuse tracing
        result = db.get_information_schema()
        logger.debug("Information schema retrieved successfully")
        return mcp.types.CallToolResult(
            content=[
                mcp.types.TextContent(type="text", text=result),
            ]
        )
    except Exception as e:
        logger.error(f"Failed to retrieve information schema: {e}")
        return mcp.types.CallToolResult(
            isError=True,
            content=[
                mcp.types.TextContent(
                    type="text",
                    text=f"Failed to retrieve information schema: {str(e)}",
                )
            ],
        )


@mcp_app.tool(
    name="Select_Query", description="Execute a select query against the OMOP database."
)
@capture_context(tool_name="Select_Query")
def read_query(query: str) -> mcp.types.CallToolResult:
    """Run a SQL query against the OMOP database.

    This function is a tool in the MCP server that allows users to execute SQL queries
    against the OMOP database. Only SELECT queries are allowed. Results are returned as CSV.

    Args:
        query: SQL query to execute
    Returns:
        Result of the query as a string or a detailed error message if the query fails.
    """
    try:
        logger.info(f"Executing query: {query[:100]}...")
        result = db.read_query(query)
        logger.info("Query executed successfully")
        return mcp.types.CallToolResult(
            content=[
                mcp.types.TextContent(type="text", text=result),
            ]
        )

    except ExceptionGroup as e:
        logger.error(f"Query validation failed: {e}")
        errors = "\n\n".join(str(i) for i in e.exceptions)
        return mcp.types.CallToolResult(
            isError=True,
            content=[
                mcp.types.TextContent(
                    type="text",
                    text=f"Query validation failed with one or more errors:\n {errors}",
                )
            ],
        )
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        return mcp.types.CallToolResult(
            isError=True,
            content=[
                mcp.types.TextContent(
                    type="text",
                    text=f"Failed to execute query: {str(e)}",
                )
            ],
        )


def main():
    """Main function to run the MCP server."""
    logger.info(f"Starting OMOP MCP Server with {transport_type.upper()} transport...")

    try:
        if transport_type == "stdio":
            mcp_app.run(transport="stdio")
        elif transport_type == "sse":
            logger.info(f"Server will be available at http://{host}:{port}")
            # Add initialization delay to prevent timing issues
            import time

            time.sleep(1)  # Give the server time to fully initialize
            mcp_app.run(transport="sse")
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)
    finally:
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    main()
