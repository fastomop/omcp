import ibis
from ibis.backends import BaseBackend

from typing import Dict, List, Optional, Any
import omcp.exceptions as ex

from functools import lru_cache
import threading
import time
import logging
from urllib.parse import urlparse, parse_qs
import databricks.sql as databricks_sql
from ibis.backends.databricks import Backend as DatabricksBackend

from omcp.sql_validator import SQLValidator
from urllib.parse import urlparse, parse_qs
import databricks.sql as databricks_sql
from ibis.backends.databricks import Backend as DatabricksBackend

logger = logging.getLogger(__name__)


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

        # Thread-safe connection handling
        self._conn_lock = threading.RLock()
        self._conn = None
        self._last_connect_time = 0
        self._connect_retry_delay = 1.0  # Start with 1 second retry

        self.conn: BaseBackend | Any = None  # Keep for backwards compatibility
        self.supported_databases = [
            "duckdb",
            "postgres",
            "databricks",
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
        self.read_only = read_only
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

        # Try initial connection
        logger.info(f"Initializing connection to: {connection_string}")
        try:
            # Check if the connection string starts with a supported prefix
            if not connection_string.startswith(tuple(self.supported_databases)):
                raise ValueError(
                    f"Unsupported database type in connection string: {connection_string}. Supported types are: {', '.join(self.supported_databases)}"
                )
            self._ensure_connected()
            # Set backwards compatible conn attribute
            self.conn = self._conn
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def _ensure_connected(self):
        """Ensure we have a valid database connection."""
        with self._conn_lock:
            # Check if we need to reconnect
            if self._conn is None or not self._is_connection_alive():
                self._reconnect()
            # Update backwards compatible conn attribute
            self.conn = self._conn

    def _is_connection_alive(self) -> bool:
        """Check if the current connection is still alive."""
        if self._conn is None:
            return False

        try:
            # Try a simple query
            self._conn.sql("SELECT 1").limit(1).execute()
            return True
        except Exception as e:
            logger.warning(f"Connection health check failed: {e}")
            return False

    def _reconnect(self):
        """Reconnect to the database with retry logic."""
        # Clean up old connection
        if self._conn:
            try:
                self._conn.disconnect()
            except Exception as disconnect_error:
                logger.warning(
                    f"Error disconnecting previous connection: {disconnect_error}"
                )
            self._conn = None

        # Retry connection with backoff
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(
                    f"Connecting to database (attempt {attempt + 1}/{max_retries})..."
                )

                if not self.connection_string.startswith(
                    tuple(self.supported_databases)
                ):
                    raise ValueError(
                        f"Unsupported database type. Supported: {', '.join(self.supported_databases)}"
                    )

                # Special handling for DuckDB to avoid locks
                if self.connection_string.startswith("duckdb://"):
                    if (
                        self.read_only
                        and "?access_mode=read_only" not in self.connection_string
                    ):
                        # Add read-only parameter if not already present
                        connection_url = (
                            f"{self.connection_string}?access_mode=read_only"
                        )
                        logger.info(
                            "Using DuckDB read-only mode to prevent file locking"
                        )
                    else:
                        connection_url = self.connection_string
                    self._conn = ibis.connect(connection_url)
                elif self.connection_string.startswith("databricks://"):
                    # Special handling for Databricks Unity Catalog without CREATE VOLUME permissions
                    # Ibis attempts to create volumes during connection initialization, which requires
                    # CREATE VOLUME permissions. This workaround bypasses that requirement by:
                    # 1. Creating a raw databricks-sql connection (which works without volumes)
                    # 2. Temporarily patching ibis's _post_connect to skip volume creation
                    # 3. Wrapping the raw connection with ibis for compatibility

                    parsed = urlparse(self.connection_string)
                    params = parse_qs(parsed.query)

                    server_hostname = params.get('server_hostname', [None])[0]
                    http_path = params.get('http_path', [None])[0]
                    access_token = params.get('access_token', [None])[0]
                    catalog = params.get('catalog', ['hive_metastore'])[0]
                    schema = params.get('schema', ['default'])[0]

                    logger.info(f"Connecting to Databricks at {server_hostname}, catalog={catalog}, schema={schema}")

                    raw_conn = databricks_sql.connect(
                        server_hostname=server_hostname,
                        http_path=http_path,
                        access_token=access_token,
                        catalog=catalog,
                        schema=schema
                    )

                    # Temporarily disable volume creation during connection
                    original_post_connect = DatabricksBackend._post_connect
                    DatabricksBackend._post_connect = lambda self, memtable_volume=None: None

                    try:
                        self._conn = ibis.databricks.from_connection(raw_conn)
                    finally:
                        DatabricksBackend._post_connect = original_post_connect
                else:
                    self._conn = ibis.connect(self.connection_string)

                # Test the connection
                self._conn.sql("SELECT 1").limit(1).execute()

                logger.info("Database connection established successfully")
                self._last_connect_time = time.time()
                self._connect_retry_delay = 1.0  # Reset retry delay
                return

            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(self._connect_retry_delay)
                    self._connect_retry_delay = min(self._connect_retry_delay * 2, 10)
                else:
                    raise ConnectionError(
                        f"Failed to connect after {max_retries} attempts: {str(e)}"
                    )

    @lru_cache(maxsize=128)
    def get_information_schema(self) -> Dict[str, List[str]]:
        """Get the information schema of the database."""
        self._ensure_connected()

        try:
            with self._conn_lock:
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
                df = self._conn.sql(query).execute()
                return df.to_csv(index=False)

        except Exception as e:
            # Clear connection on error
            self._conn = None
            self.conn = None
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
            # Validate the SQL query first (no DB connection needed)
            errors = self.sql_validator.validate_sql(query)

            # DoNotDelete: Adding message and exceptions keywords to the exception group
            # results in `TypeError: BaseExceptionGroup.__new__() takes exactly 2 arguments (0 given)`
            if errors:
                raise ExceptionGroup(
                    "Query validation failed",
                    errors,
                )

            # Ensure connected
            self._ensure_connected()

            # Execute the validated query
            with self._conn_lock:
                result = self._conn.sql(query).limit(self.row_limit)
                df = result.execute()
                # Convert dataframe to csv
                return df.to_csv(index=False)

        except ExceptionGroup:
            raise
        except Exception:
            # Clear connection on error
            self._conn = None
            self.conn = None

            # Try one more time after reconnecting
            try:
                self._ensure_connected()
                with self._conn_lock:
                    result = self._conn.sql(query).limit(self.row_limit)
                    df = result.execute()
                    return df.to_csv(index=False)
            except Exception as retry_error:
                raise ex.QueryError(f"Failed to execute query: {str(retry_error)}")

    def __del__(self):
        """Clean up connection on deletion."""
        if self._conn:
            try:
                self._conn.disconnect()
                logger.debug("Database connection closed during cleanup")
            except Exception as cleanup_error:
                logger.warning(
                    f"Error closing connection during cleanup: {cleanup_error}"
                )
