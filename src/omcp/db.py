import ibis
from ibis.backends import BaseBackend
from ibis.backends.duckdb import Backend as DuckDBBackend
from typing import Dict, List, Optional
from sqlglot import expressions as exp
from sqlglot import parse_one


class OmopDatabase:
    """
    A class for interacting with an OMOP database using the Ibis framework.
    """

    def __init__(self, connection_string: str, read_only=True, allow_source_values: bool = False, allowed_tables: Optional[List[str]] = None):
        """
        Initialize the database connection.

        Args:
            connection_string: SQL connection string for the database
            read_only: Flag to set the connection as read-only
            allow_source_values: Flag to allow source values
            allowed_tables: List of allowed tables for queries
        """

        self.conn: BaseBackend | DuckDBBackend = None
        self.supported_databases = [
            "duckdb",
            # 'postgresql',
            # 'mssql',
            # 'mysql',
            # 'sqlite',
            # 'clickhouse',
            # 'bigquery',
            # 'snowflake',
            # 'impala',
            # 'oracle'
        ]
        self.connection_string = connection_string
        self.row_limit = 1000  # Default row limit for queries
        self.allowed_tables = allowed_tables if allowed_tables is not None else [
            "care_site",
            "cdm_source",
            "concept",
            "concept_ancestor",
            "concept_class",
            "concept_relationship",
            "concept_synonym",
            "condition_era",
            "condition_occurrence",
            "cost",
            "death",
            "device_exposure",
            "domain",
            "dose_era",
            "drug_era",
            "drug_exposure",
            "drug_strength",
            "episode",
            "episode_event",
            "fact_relationship",
            "location",
            "measurement",
            "metadata",
            "note",
            "note_nlp",
            "observation",
            "observation_period",
            "payer_plan_period",
            "person",
            "procedure_occurrence",
            "provider",
            "relationship",
            "specimen",
            "visit_detail",
            "visit_occurrence",
            "vocabulary"
        ]

        self.allow_source_values:bool = allow_source_values

        try:
            # Parse connection string to determine database type and connect
            if connection_string.startswith("duckdb://"):
            
                import importlib.util
                if not importlib.util.find_spec("ibis.backends.duckdb"):
                    raise ImportError("The 'ibis.backends.duckdb' module is not installed. Please install it to use this backend.")
                self.conn = ibis.duckdb.connect(
                    connection_string.replace("duckdb://", ""), read_only=read_only 
                )
            # Uncomment and modify the following lines to support other databases after testing
            # elif connection_string.startswith('postgresql://'):
            
            #     self.conn: ibis.backends.postgres.Backend = ibis.postgres.connect(connection_string)
            # elif connection_string.startswith('mssql://') or connection_string.startswith('sqlserver://'):
            #     self.conn: ibis.backends.mssql.Backend = ibis.mssql.connect(connection_string)
            # elif connection_string.startswith('mysql://'):
            #     self.conn: ibis.backends.mysql.Backend = ibis.mysql.connect(connection_string)
            # elif connection_string.startswith('sqlite://'):
            #     self.conn: ibis.backends.sqlite.Backend = ibis.sqlite.connect(connection_string.replace('sqlite://', ''))
            # # elif connection_string.startswith('clickhouse://'):
            #     self.conn: ibis.backends.clickhouse.Backend = ibis.clickhouse.connect(connection_string)
            # elif connection_string.startswith('bigquery://'):
            #     self.conn: ibis.backends.bigquery.Backend = ibis.bigquery.connect(connection_string.replace('bigquery://', ''))
            # elif connection_string.startswith('snowflake://'):
            #     self.conn: ibis.backends.snowflake.Backend = ibis.snowflake.connect(connection_string)
            # elif connection_string.startswith('impala://'):
            #     self.conn: ibis.backends.impala.Backend      = ibis.impala.connect(connection_string)
            # elif connection_string.startswith('oracle://'):
            #     self.conn: ibis.backends.oracle.Backend = ibis.oracle.connect(connection_string)
            else:
                raise ValueError(
                    f"Unsupported database type in connection string: {connection_string}. Supported types are: {', '.join(self.supported_databases)}"
                )

        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def get_information_schema(self) -> Dict[str, List[str]]:
        """Get the information schema of the database."""
        try:
            at = ",".join(f"'{i}'" for i in self.allowed_tables)
            query = f"""select table_schema, table_name, column_name,data_type from information_schema.columns where table_name in ({at})"""
            # Add filtering for source_value columns if not allowed
            if not self.allow_source_values:
                query += " and lower(column_name) not like '%_source_value%'"

            query += ";"  # Add the semicolon to terminate the SQL query
            df = self.conn.sql(query).execute()
            return df.to_csv(index=False)
           
        except Exception as e:
            raise QueryError(f"Failed to get information schema: {str(e)}")

    def read_query(self, query: str) -> str:
        """
        Execute a read-only SQL query and return results as CSV

        Args:
            query: SQL query string

        Returns:
            CSV string representing query results
        """
        try:
            parsed_query = parse_one(query)
            # Validate the query to ensure it's a SELECT statement
            if not isinstance(parsed_query, exp.Select):
                raise ValueError("Query must be a SELECT statement.")

            # Execute the validated query
            result = self.conn.sql(query).limit(
                self.row_limit
            )  # Limit to 1000 rows for performance
            df = result.execute()
            # Convert dataframe to csv
            return df.to_csv(index=False)
        except Exception as e:
            raise QueryError(f"Failed to execute read query: {str(e)}")

    # Potentially useful for future use.
    # def execute_query(self, query: str) -> Optional[str]:
    #     """
    #     Execute any SQL query including INSERT, UPDATE, DELETE, etc.

    #     Args:
    #         query: Any SQL query string

    #     Returns:
    #         JSON results for SELECT queries, None for other query types
    #     """
    #     try:
    #         # For SELECT queries, we can return results
    #         if isinstance(parse_one(query), exp.Select):
    #             result = self.conn.sql(query).limit(self.row_limit)  # Limit to 1000 rows for performance
    #             df = result.execute()
    #             # Convert dataframe to csv
    #             return df.to_csv(index=False)
    #         else:
    #             # For non-SELECT queries, execute raw SQL with no return value expected
    #             self.conn.raw_sql(query)
    #             return None
    #     except Exception as e:
    #         raise QueryError(f"Failed to execute query: {str(e)}")


class QueryError(Exception):
    """Exception raised for errors in the query execution"""

    pass
