"""
Robust OMOP Database handler with connection recovery and proper error handling.
"""

import ibis
from ibis.backends import BaseBackend
from typing import Dict, List, Optional, Any
import omcp.exceptions as ex
from functools import lru_cache
import threading
import time
import logging
from omcp.sql_validator import SQLValidator

logger = logging.getLogger(__name__)

class RobustOmopDatabase:
    """
    A robust database class with automatic reconnection and better error handling.
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
        """Initialize with connection parameters."""
        self.connection_string = connection_string
        self.read_only = read_only
        self.cdm_schema = cdm_schema
        self.vocab_schema = vocab_schema
        self.allow_source_value_columns = allow_source_value_columns
        self.row_limit = 1000
        
        # Thread-safe connection handling
        self._conn_lock = threading.RLock()
        self._conn = None
        self._last_connect_time = 0
        self._connect_retry_delay = 1.0  # Start with 1 second retry
        
        self.supported_databases = ["duckdb", "postgres"]
        self.allowed_tables = allowed_tables or [
            "care_site", "concept", "concept_ancestor", "concept_class",
            "concept_relationship", "concept_synonym", "condition_occurrence",
            "death", "domain", "drug_exposure", "location", "measurement",
            "observation", "person", "procedure_occurrence", "provider",
            "relationship", "specimen", "visit_detail", "visit_occurrence",
            "vocabulary",
        ]
        
        self.sql_validator = SQLValidator(
            allow_source_value_columns=self.allow_source_value_columns,
            exclude_tables=None,
            exclude_columns=None,
        )
        
        # Try initial connection
        self._ensure_connected()
    
    def _ensure_connected(self):
        """Ensure we have a valid database connection."""
        with self._conn_lock:
            # Check if we need to reconnect
            if self._conn is None or not self._is_connection_alive():
                self._reconnect()
    
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
            except:
                pass
            self._conn = None
        
        # Retry connection with backoff
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to database (attempt {attempt + 1}/{max_retries})...")
                
                if not self.connection_string.startswith(tuple(self.supported_databases)):
                    raise ValueError(
                        f"Unsupported database type. Supported: {', '.join(self.supported_databases)}"
                    )
                
                # Special handling for DuckDB to avoid locks
                if self.connection_string.startswith("duckdb://"):
                    if self.read_only and "?access_mode=read_only" not in self.connection_string:
                        # Add read-only parameter if not already present
                        connection_url = f"{self.connection_string}?access_mode=read_only"
                        logger.info("Using DuckDB read-only mode to prevent file locking")
                    else:
                        connection_url = self.connection_string
                    self._conn = ibis.connect(connection_url)
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
                    raise ConnectionError(f"Failed to connect after {max_retries} attempts: {str(e)}")
    
    @lru_cache(maxsize=128)
    def get_information_schema(self) -> str:
        """Get the information schema with automatic reconnection."""
        self._ensure_connected()
        
        try:
            with self._conn_lock:
                at = ",".join(f"'{i}'" for i in self.allowed_tables)
                query = f"""
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_name in ({at})
                AND table_schema in ('{self.cdm_schema}', '{self.vocab_schema}')
                """
                
                if not self.allow_source_value_columns:
                    query += " AND lower(column_name) NOT LIKE '%_source_value%'"
                
                query += ";"
                
                df = self._conn.sql(query).execute()
                return df.to_csv(index=False)
                
        except Exception as e:
            # Clear connection on error
            self._conn = None
            raise ex.QueryError(f"Failed to get information schema: {str(e)}")
    
    def read_query(self, query: str) -> str:
        """Execute a query with automatic reconnection on failure."""
        # Validate first (no DB connection needed)
        errors = self.sql_validator.validate_sql(query)
        if errors:
            raise ExceptionGroup("Query validation failed", errors)
        
        # Ensure connected
        self._ensure_connected()
        
        try:
            with self._conn_lock:
                result = self._conn.sql(query).limit(self.row_limit)
                df = result.execute()
                return df.to_csv(index=False)
                
        except ExceptionGroup:
            raise
        except Exception as e:
            # Clear connection on error
            self._conn = None
            
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
            except:
                pass

# Make it compatible with existing code
OmopDatabase = RobustOmopDatabase