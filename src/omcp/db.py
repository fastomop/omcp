import ibis
from ibis.backends import BaseBackend

from typing import Dict, List, Optional, Any
import omcp.exceptions as ex

from functools import lru_cache

from omcp.sql_validator import SQLValidator


class OmopDatabase:
    """
    A class for interacting with an OMOP database using the Ibis framework.
    """

    def __init__(
        self,
        connection_string: str,
        read_only=True,
        cdm_schema: str = "cdm",
        vocab_schema: str = "vocab",
        allow_source_value_columns: bool = False,
        allowed_tables: Optional[List[str]] = None,
    ):
        """
        Initialize the database connection.

        Args:
            connection_string: SQL connection string for the database
            read_only: Flag to set the connection as read-only
            allow_source_values: Flag to allow source values
            allowed_tables: List of allowed tables for queries
        """

        self.conn: BaseBackend | Any = None
        self.supported_databases = [
            "duckdb",
            "postgres",
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
        self.allowed_tables = allowed_tables or [
            "care_site",
            # "cdm_source",
            "concept",
            "concept_ancestor",
            "concept_class",
            "concept_relationship",
            "concept_synonym",
            # "condition_era",
            "condition_occurrence",
            # "cost",
            "death",
            # "device_exposure",
            "domain",
            # "dose_era",
            # "drug_era",
            "drug_exposure",
            # "drug_strength",
            # "episode",
            # "episode_event",
            # "fact_relationship",
            "location",
            "measurement",
            # "metadata",
            # "note",
            # "note_nlp",
            "observation",
            # "observation_period",
            # "payer_plan_period",
            "person",
            "procedure_occurrence",
            "provider",
            "relationship",
            "specimen",
            "visit_detail",
            "visit_occurrence",
            "vocabulary",
        ]

        self.allow_source_value_columns: bool = allow_source_value_columns

        self.sql_validator = SQLValidator(
            allow_source_value_columns=self.allow_source_value_columns,
            exclude_tables=None,
            exclude_columns=None,
        )
        self.cdm_schema = cdm_schema
        self.vocab_schema = vocab_schema
        print(connection_string)
        try:
            # Check if the connection string starts with a supported prefix
            if not connection_string.startswith(tuple(self.supported_databases)):
                raise ValueError(
                    f"Unsupported database type in connection string: {connection_string}. Supported types are: {', '.join(self.supported_databases)}"
                )
            self.conn = ibis.connect(connection_string)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    @lru_cache(maxsize=128)
    def get_information_schema(self) -> Dict[str, List[str]]:
        """Get the information schema of the database."""
        try:
            at = ",".join(f"'{i}'" for i in self.allowed_tables)
            query = f"""
            select table_schema, table_name, column_name,data_type
            from information_schema.columns
            where table_name in ({at})
            and table_schema in ('{self.cdm_schema}', '{self.vocab_schema}')
            """
            # Add filtering for source_value columns if not allowed
            if not self.allow_source_value_columns:
                query += " and lower(column_name) not like '%_source_value%'"

            query += ";"  # Add the semicolon to terminate the SQL query
            df = self.conn.sql(query).execute()
            return df.to_csv(index=False)

        except Exception as e:
            raise ex.QueryError(f"Failed to get information schema: {str(e)}")

    @lru_cache(maxsize=128)
    def read_query(self, query: str) -> str:
        """
        Execute a read-only SQL query and return results as CSV

        Args:
            query: SQL query string

        Returns:
            CSV string representing query results
        """

        try:
            # Validate the SQL query
            errors = self.sql_validator.validate_sql(query)

            # DoNotDelete: Adding message and exceptions keywords to the exception group
            # results in `TypeError: BaseExceptionGroup.__new__() takes exactly 2 arguments (0 given)`
            if errors:
                raise ExceptionGroup(
                    "Query validation failed",
                    errors,
                )

            # Execute the validated query
            result = self.conn.sql(query).limit(
                self.row_limit
            )  # Limit to 1000 rows for performance
            df = result.execute()
            # Convert dataframe to csv
            return df.to_csv(index=False)

        except ExceptionGroup:
            raise
        except Exception as e:
            raise ex.QueryError(f"Failed to execute query: {str(e)}")
