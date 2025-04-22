import os
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv, find_dotenv
from omcp.db import OmopDatabase
from omcp.exceptions import (
    AmbiguousReferenceError,
    ColumnNotFoundError,
    EmptyQueryError,
    NotSelectQueryError,
    QueryError,
    SqlSyntaxError,
    TableNotFoundError,
    UnauthorizedTableError,
)

load_dotenv(find_dotenv())


connection_string = os.environ["DB_CONNECTION_STRING"]

# Default host and port values, can be overridden via environment variables
host = os.environ.get("MCP_HOST", "localhost")
port = int(os.environ.get("MCP_PORT", "8000"))

mcp = FastMCP(name="OMOP MCP Server")
db = OmopDatabase(connection_string=connection_string)


@mcp.tool(
    name="Get_Information_Schema",
    description="Get the information schema of the OMOP database.",
)
def get_information_schema() -> list[str]:
    """Get the information schema of the OMOP database.

    This function is a tool in the MCP server that retrieves the information schema
    from the OMOP database. Information is restricted to only tables and columns allowed by the
    configuration. The function returns a list of schemas, tables, columns and data types
    in the OMOP database formatted as a CSV string.
    Args:
        None
    Returns:
        List of schemas, tables, columns and data types in the OMOP database formatted as a CSV string.
    """
    return db.get_information_schema()


@mcp.tool(
    name="Select_Query", description="Execute a select query against the OMOP database."
)
def read_query(query: str) -> str:
    """Run a SQL query against the OMOP database.

    This function is a tool in the MCP server that allows users to execute SQL queries
    against the OMOP database. Only SELECT queries are allowed. Results are returned as CSV.

    Args:
        query: SQL query to execute
    Returns:
        Result of the query as a string or a detailed error message if the query fails.
    """
    try:
        return db.read_query(query)
    except EmptyQueryError as e:
        return f"Error: {str(e)}. Please provide a non-empty SQL query."
    except SqlSyntaxError as e:
        return f"Error: {str(e)}. Please check your SQL syntax and try again."
    except NotSelectQueryError as e:
        return (
            f"Error: {str(e)}. For security reasons, only SELECT queries are allowed."
        )
    except UnauthorizedTableError as e:
        return f"Error: {str(e)}. Please use only the authorized tables."
    except ColumnNotFoundError as e:
        return f"Error: {str(e)}. Please check column names and table references."
    except TableNotFoundError as e:
        return f"Error: {str(e)}. Please check that you're using valid table names."
    except AmbiguousReferenceError as e:
        return f"Error: {str(e)}. Please qualify column names with table names to resolve ambiguity."
    except QueryError as e:
        return f"Error executing query: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}. Please contact the administrator if this issue persists."


def main():
    """Main function to run the MCP server."""
    print(f"Starting OMOP MCP Server with SSE transport on {host}:{port}")

    # Run the server with SSE transport
    mcp.run(
        transport="sse",
        # host=host,
        # port=port
    )


if __name__ == "__main__":
    main()
