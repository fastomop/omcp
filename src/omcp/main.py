import os
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv, find_dotenv
from omcp.db import OmopDatabase

load_dotenv(find_dotenv())


connection_string = os.environ["DB_CONNECTION_STRING"]

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
    against the OMOP database. Only SELECT queries are allowed. Resulsts are returned as CSV.
    Args:
        query: SQL query to execute
    Returns:
        Result of the query as a string or the exception message if the query fails.
    """

    return db.read_query(query)


def main():
    """Main function to run the MCP server."""


    # Run the server
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
