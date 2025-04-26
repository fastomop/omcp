import os
from mcp.server.fastmcp import FastMCP
import mcp
from dotenv import load_dotenv, find_dotenv
from omcp.db import OmopDatabase

load_dotenv(find_dotenv())


connection_string = os.environ["DB_CONNECTION_STRING"]

# Default host and port values, can be overridden via environment variables
host = os.environ.get("MCP_HOST", "localhost")
port = int(os.environ.get("MCP_PORT", "8000"))

mcp_app = FastMCP(name="OMOP MCP Server")
db = OmopDatabase(
    connection_string=connection_string,
    cdm_schema=os.environ.get("CDM_SCHEMA", "base"),
    vocab_schema=os.environ.get("VOCAB_SCHEMA", "base"),
)


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
        result = db.get_information_schema()
        return mcp.types.CallToolResult(
            content=[
                mcp.types.TextContent(type="text", text=result),
            ]
        )
    except Exception as e:
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
        result = db.read_query(query)
        return mcp.types.CallToolResult(
            content=[
                mcp.types.TextContent(type="text", text=result),
            ]
        )

    except ExceptionGroup as e:
        errors = "\n\n".join(i.message for i in e.exceptions)
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
    print(f"Starting OMOP MCP Server with SSE transport on {host}:{port}")

    # Run the server with SSE transport
    mcp_app.run(
        transport="stdio",
    )


if __name__ == "__main__":
    main()
