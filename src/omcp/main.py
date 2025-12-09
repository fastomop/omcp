import os
from mcp.server.fastmcp import FastMCP
import mcp
from dotenv import load_dotenv, find_dotenv
from omcp.db import OmopDatabase

load_dotenv(find_dotenv())


db_type = os.environ.get("DB_TYPE")
db_path = os.environ.get("DB_PATH")

if db_type == "duckdb":
    connection_string = f"duckdb:///{db_path}"
elif db_type == "postgres":
    # Assuming standard postgres env vars are set for full connection string
    # This part would need more detail if you want to support full postgres connection string construction
    connection_string = f"postgresql://{os.environ.get('DB_USERNAME')}:{os.environ.get('DB_PASSWORD')}@{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT')}/{os.environ.get('DB_DATABASE')}"
else:
    raise ValueError("Unsupported DB_TYPE. Must be 'duckdb' or 'postgres'.")

# Default host and port values, can be overridden via environment variables
host = os.environ.get("MCP_HOST", "localhost")
port = int(os.environ.get("MCP_PORT", "8080"))

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
        return mcp.types.CallToolResult(
            isError=True,
            content=[
                mcp.types.TextContent(
                    type="text",
                    text=f"Failed to execute query: {str(e)}",
                )
            ],
        )


<<<<<<< Updated upstream
=======
@mcp_app.tool(
    name="Lookup_Drug",
    description="Look up drug concepts by name in the OMOP concept table. Returns standardized drug concepts with concept_id, concept_name, concept_code, vocabulary_id, and domain_id. Only searches standard RxNorm vocabulary.",
)
@capture_context(tool_name="Lookup_Drug")
def lookup_drug(term: str, limit: int = 10) -> mcp.types.CallToolResult:
    """Look up drug concepts by name.

    This function searches for drug concepts in the OMOP concept table by partial name match.
    Only returns standard, valid drug concepts from RxNorm vocabulary, ordered by name length (shortest first).
    Excludes non-standard vocabularies like RxNorm Extension to ensure compatibility.

    Args:
        term: Drug name to search for (case-insensitive partial match)
        limit: Maximum number of results to return (default: 10)

    Returns:
        CSV formatted results with: concept_id, concept_name, concept_code, vocabulary_id, domain_id
    """
    try:
        schema = db.cdm_schema
        # Filter to RxNorm vocabulary only - excludes RxNorm Extension and other non-standard vocabularies
        query = f"""
        SELECT concept_id, concept_name, concept_code, vocabulary_id, domain_id
        FROM {schema}.concept
        WHERE LOWER(concept_name) LIKE LOWER('%{term}%')
          AND domain_id = 'Drug'
          AND vocabulary_id = 'RxNorm'
          AND standard_concept = 'S'
          AND invalid_reason IS NULL
        ORDER BY LENGTH(concept_name), concept_name
        LIMIT {limit}
        """
        logger.info(f"Looking up drug: {term}")
        result = db.read_query(query)
        logger.info(f"Drug lookup completed for: {term}")
        return mcp.types.CallToolResult(
            content=[mcp.types.TextContent(type="text", text=result)]
        )
    except Exception as e:
        logger.error(f"Failed to lookup drug '{term}': {e}")
        return mcp.types.CallToolResult(
            isError=True,
            content=[
                mcp.types.TextContent(
                    type="text", text=f"Failed to lookup drug: {str(e)}"
                )
            ],
        )


@mcp_app.tool(
    name="Lookup_Condition",
    description="Look up condition concepts by name in the OMOP concept table. Returns standardized condition concepts with concept_id, concept_name, concept_code, vocabulary_id, and domain_id. Only searches standard SNOMED vocabulary.",
)
@capture_context(tool_name="Lookup_Condition")
def lookup_condition(term: str, limit: int = 10) -> mcp.types.CallToolResult:
    """Look up condition concepts by name.

    This function searches for condition concepts in the OMOP concept table by partial name match.
    Only returns standard, valid condition concepts from SNOMED vocabulary, ordered by name length (shortest first).
    Filters to SNOMED CT vocabulary to ensure compatibility across OMOP databases.

    Args:
        term: Condition name to search for (case-insensitive partial match)
        limit: Maximum number of results to return (default: 10)

    Returns:
        CSV formatted results with: concept_id, concept_name, concept_code, vocabulary_id, domain_id
    """
    try:
        schema = db.cdm_schema
        # Filter to SNOMED vocabulary only - standard vocabulary for conditions
        query = f"""
        SELECT concept_id, concept_name, concept_code, vocabulary_id, domain_id
        FROM {schema}.concept
        WHERE LOWER(concept_name) LIKE LOWER('%{term}%')
          AND domain_id = 'Condition'
          AND vocabulary_id = 'SNOMED'
          AND standard_concept = 'S'
          AND invalid_reason IS NULL
        ORDER BY LENGTH(concept_name), concept_name
        LIMIT {limit}
        """
        logger.info(f"Looking up condition: {term}")
        result = db.read_query(query)
        logger.info(f"Condition lookup completed for: {term}")
        return mcp.types.CallToolResult(
            content=[mcp.types.TextContent(type="text", text=result)]
        )
    except Exception as e:
        logger.error(f"Failed to lookup condition '{term}': {e}")
        return mcp.types.CallToolResult(
            isError=True,
            content=[
                mcp.types.TextContent(
                    type="text", text=f"Failed to lookup condition: {str(e)}"
                )
            ],
        )


>>>>>>> Stashed changes
def main():
    """Main function to run the MCP server."""

    mcp_app.run(
        transport="stdio",
    )


if __name__ == "__main__":
    main()
