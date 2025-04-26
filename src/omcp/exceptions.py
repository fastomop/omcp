# Define more specific error classes for better error handling


class QueryError(Exception):
    """Base exception raised for errors in the query execution"""

    pass


class AmbiguousReferenceError(QueryError):
    """Exception raised when a column reference is ambiguous"""

    pass


class ColumnNotFoundError(QueryError):
    """Exception raised when a column referenced in the query doesn't exist"""

    pass


class EmptyQueryError(QueryError):
    """Exception raised when the query is empty"""

    pass


class NotSelectQueryError(QueryError):
    """Exception raised when a non-SELECT query is attempted"""

    pass


class SqlSyntaxError(QueryError):
    """Exception raised for SQL syntax errors"""

    pass


class StarNotAllowedError(QueryError):
    """Exception raised when a star (*) is used in the query"""

    pass


class TableNotFoundError(QueryError):
    """Exception raised when a table referenced in the query doesn't exist"""

    pass


class UnauthorizedTableError(QueryError):
    """Exception raised when query attempts to access unauthorized tables"""

    pass


class UnauthorizedColumnError(QueryError):
    """Exception raised when query attempts to access unauthorized columns"""

    pass
