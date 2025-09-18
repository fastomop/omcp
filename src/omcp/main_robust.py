"""
Robust OMCP server with better error handling and graceful shutdown.
"""

import os
import sys
import signal
import logging
from mcp.server.fastmcp import FastMCP
import mcp
from dotenv import load_dotenv, find_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import the robust database
try:
    from omcp.db_robust import RobustOmopDatabase as OmopDatabase
except ImportError:
    logger.warning("Robust database not found, falling back to original")
    from omcp.db import OmopDatabase

load_dotenv(find_dotenv())

# Global variable for graceful shutdown
db = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    if db and hasattr(db, '_conn') and db._conn:
        try:
            db._conn.disconnect()
            logger.info("Database connection closed")
        except:
            pass
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

# Initialize FastMCP
mcp_app = FastMCP(name="OMOP MCP Server")

# Initialize database with robust handling
try:
    db = OmopDatabase(
        connection_string=connection_string,
        cdm_schema=os.environ.get("CDM_SCHEMA", "base"),
        vocab_schema=os.environ.get("VOCAB_SCHEMA", "base"),
        read_only=db_read_only
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
    """Get the information schema of the OMOP database."""
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
    name="Select_Query", 
    description="Execute a select query against the OMOP database."
)
def read_query(query: str) -> mcp.types.CallToolResult:
    """Run a SQL query against the OMOP database."""
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
    logger.info("Starting OMOP MCP Server...")
    
    try:
        mcp_app.run(transport="stdio")
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)
    finally:
        logger.info("Server shutdown complete")

if __name__ == "__main__":
    main()