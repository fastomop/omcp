"""
OMCP Exception Module

This module defines custom exception classes for better error handling in the OMCP
(OMOP Model Context Protocol) server. All exceptions inherit from a base QueryError
class and provide specific error types for different validation and execution scenarios.
"""

from typing import List, Optional, Dict, Any


class QueryError(Exception):
    """
    Base exception raised for errors in query execution and validation.

    This is the parent class for all OMCP-specific exceptions. It provides
    a standardized way to handle errors with additional context.
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the QueryError.

        Args:
            message: Human-readable error message
            details: Optional dictionary containing additional error context
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        """Return a string representation of the error."""
        if self.details:
            detail_str = ", ".join(f"{k}: {v}" for k, v in self.details.items())
            return f"{self.message} (Details: {detail_str})"
        return self.message


class AmbiguousReferenceError(QueryError):
    """
    Exception raised when a column reference is ambiguous.

    This occurs when a column name could refer to multiple tables in a query
    without proper table qualification.
    """

    def __init__(
        self,
        message: str,
        column_name: Optional[str] = None,
        table_candidates: Optional[List[str]] = None,
    ):
        """
        Initialize the AmbiguousReferenceError.

        Args:
            message: Error message
            column_name: The ambiguous column name
            table_candidates: List of tables that contain the column
        """
        details = {}
        if column_name:
            details["column_name"] = column_name
        if table_candidates:
            details["table_candidates"] = table_candidates
        super().__init__(message, details)


class ColumnNotFoundError(QueryError):
    """
    Exception raised when a column referenced in the query doesn't exist.

    This error occurs when a query references a column that is not present
    in any of the tables being queried.
    """

    def __init__(
        self,
        message: str,
        column_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ):
        """
        Initialize the ColumnNotFoundError.

        Args:
            message: Error message
            column_name: The missing column name
            table_name: The table where the column was expected
        """
        details = {}
        if column_name:
            details["column_name"] = column_name
        if table_name:
            details["table_name"] = table_name
        super().__init__(message, details)


class EmptyQueryError(QueryError):
    """
    Exception raised when the query is empty or contains only whitespace.

    This error prevents execution of empty queries which could cause
    unexpected behavior.
    """

    def __init__(self, message: str = "Query cannot be empty"):
        """Initialize the EmptyQueryError."""
        super().__init__(message)


class NotSelectQueryError(QueryError):
    """
    Exception raised when a non-SELECT query is attempted.

    For security reasons, only SELECT statements are allowed in the OMCP server.
    This exception is raised when other SQL statement types are detected.
    """

    def __init__(self, message: str, query_type: Optional[str] = None):
        """
        Initialize the NotSelectQueryError.

        Args:
            message: Error message
            query_type: The type of query that was attempted (INSERT, UPDATE, etc.)
        """
        details = {}
        if query_type:
            details["attempted_query_type"] = query_type
        super().__init__(message, details)


class SqlSyntaxError(QueryError):
    """
    Exception raised for SQL syntax errors.

    This wraps SQL parsing errors to provide consistent error handling
    within the OMCP framework.
    """

    def __init__(
        self,
        message: str,
        line_number: Optional[int] = None,
        column_number: Optional[int] = None,
    ):
        """
        Initialize the SqlSyntaxError.

        Args:
            message: Error message
            line_number: Line number where the syntax error occurred
            column_number: Column number where the syntax error occurred
        """
        details = {}
        if line_number is not None:
            details["line_number"] = line_number
        if column_number is not None:
            details["column_number"] = column_number
        super().__init__(message, details)


class StarNotAllowedError(QueryError):
    """
    Exception raised when a star (*) is used in the query.

    For security and performance reasons, SELECT * queries may be
    restricted in certain configurations.
    """

    def __init__(self, message: str = "SELECT * is not allowed for security reasons"):
        """Initialize the StarNotAllowedError."""
        super().__init__(message)


class TableNotFoundError(QueryError):
    """
    Exception raised when a table referenced in the query doesn't exist.

    This occurs when a query references tables that are not part of the
    OMOP CDM or are not available in the current database schema.
    """

    def __init__(
        self,
        message: str,
        table_names: Optional[List[str]] = None,
        schema_name: Optional[str] = None,
    ):
        """
        Initialize the TableNotFoundError.

        Args:
            message: Error message
            table_names: List of table names that were not found
            schema_name: The schema where tables were expected
        """
        details = {}
        if table_names:
            details["missing_tables"] = table_names
        if schema_name:
            details["schema_name"] = schema_name
        super().__init__(message, details)


class UnauthorizedTableError(QueryError):
    """
    Exception raised when query attempts to access unauthorized tables.

    This security exception prevents access to tables that have been
    explicitly excluded from the allowed table list.
    """

    def __init__(self, message: str, unauthorized_tables: Optional[List[str]] = None):
        """
        Initialize the UnauthorizedTableError.

        Args:
            message: Error message
            unauthorized_tables: List of unauthorized table names
        """
        details = {}
        if unauthorized_tables:
            details["unauthorized_tables"] = unauthorized_tables
        super().__init__(message, details)


class UnauthorizedColumnError(QueryError):
    """
    Exception raised when query attempts to access unauthorized columns.

    This security exception prevents access to columns that have been
    explicitly excluded, such as source_value columns or other restricted fields.
    """

    def __init__(
        self,
        message: str,
        unauthorized_columns: Optional[List[str]] = None,
        column_type: Optional[str] = None,
    ):
        """
        Initialize the UnauthorizedColumnError.

        Args:
            message: Error message
            unauthorized_columns: List of unauthorized column names
            column_type: Type of restricted column (e.g., "source_value")
        """
        details = {}
        if unauthorized_columns:
            details["unauthorized_columns"] = unauthorized_columns
        if column_type:
            details["column_type"] = column_type
        super().__init__(message, details)


class DatabaseConnectionError(QueryError):
    """
    Exception raised when database connection fails.

    This exception handles various database connectivity issues including
    network problems, authentication failures, and configuration errors.
    """

    def __init__(
        self,
        message: str,
        connection_string: Optional[str] = None,
        error_code: Optional[str] = None,
    ):
        """
        Initialize the DatabaseConnectionError.

        Args:
            message: Error message
            connection_string: The connection string that failed (sensitive info removed)
            error_code: Database-specific error code
        """
        details = {}
        if connection_string:
            # Sanitize connection string to remove sensitive information
            sanitized = (
                connection_string.split("://")[0] + "://[REDACTED]"
                if "://" in connection_string
                else "[REDACTED]"
            )
            details["connection_type"] = sanitized
        if error_code:
            details["error_code"] = error_code
        super().__init__(message, details)


class ValidationError(QueryError):
    """
    Exception raised for general validation errors.

    This is a catch-all exception for validation issues that don't fit
    into more specific categories.
    """

    def __init__(
        self,
        message: str,
        validation_type: Optional[str] = None,
        failed_checks: Optional[List[str]] = None,
    ):
        """
        Initialize the ValidationError.

        Args:
            message: Error message
            validation_type: Type of validation that failed
            failed_checks: List of specific validation checks that failed
        """
        details = {}
        if validation_type:
            details["validation_type"] = validation_type
        if failed_checks:
            details["failed_checks"] = failed_checks
        super().__init__(message, details)


class QueryTimeoutError(QueryError):
    """
    Exception raised when a query execution times out.

    This exception is raised when query execution exceeds the configured
    timeout limit, helping to prevent resource exhaustion.
    """

    def __init__(self, message: str, timeout_seconds: Optional[float] = None):
        """
        Initialize the QueryTimeoutError.

        Args:
            message: Error message
            timeout_seconds: The timeout limit that was exceeded
        """
        details = {}
        if timeout_seconds is not None:
            details["timeout_seconds"] = timeout_seconds
        super().__init__(message, details)


class RowLimitExceededError(QueryError):
    """
    Exception raised when query results exceed the configured row limit.

    This exception helps prevent memory issues and ensures reasonable
    response times by limiting result set sizes.
    """

    def __init__(
        self,
        message: str,
        row_limit: Optional[int] = None,
        actual_rows: Optional[int] = None,
    ):
        """
        Initialize the RowLimitExceededError.

        Args:
            message: Error message
            row_limit: The configured row limit
            actual_rows: The actual number of rows that would be returned
        """
        details = {}
        if row_limit is not None:
            details["row_limit"] = row_limit
        if actual_rows is not None:
            details["actual_rows"] = actual_rows
        super().__init__(message, details)
