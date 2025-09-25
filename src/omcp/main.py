import os
import sys
import signal
import logging
from mcp.server.fastmcp import FastMCP
import mcp
from dotenv import load_dotenv, find_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
else:
    raise ValueError("Unsupported DB_TYPE. Must be 'duckdb' or 'postgres'.")

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
            mcp_app.run(transport="sse", host=host, port=port)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)
    finally:
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    main()
