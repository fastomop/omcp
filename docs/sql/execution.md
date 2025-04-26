# Query Execution

This page explains how SQL queries are executed against OMOP databases in the OMCP project. The database layer handles connections, validation, and query execution, ensuring secure and efficient data access.

## OmopDatabase Class

The core of database connectivity is the `OmopDatabase` class in the `db.py` module. This class:

1. Establishes connections to various database backends
2. Validates SQL queries through the `SQLValidator`
3. Executes safe queries and returns formatted results

## Initialization

The `OmopDatabase` is initialized with connection details and security parameters:

```python
def __init__(
    self,
    connection_string: str,
    read_only=True,
    cdm_schema: str = "cdm",
    vocab_schema: str = "vocab",
    allow_source_value_columns: bool = False,
    allowed_tables: Optional[List[str]] = None,
):
```

| Parameter | Description |
| --------- | ----------- |
| `connection_string` | Database URI (e.g., `duckdb://path/to/file.duckdb`) |
| `read_only` | Whether to open the connection in read-only mode |
| `cdm_schema` | Schema name for clinical data tables |
| `vocab_schema` | Schema name for vocabulary tables |
| `allow_source_value_columns` | Whether to allow querying source value columns |
| `allowed_tables` | List of specific tables to allow (defaults to standard OMOP tables) |

## Database Connection

The class supports multiple database backends through the Ibis framework, with DuckDB as the current default implementation:

```python
if connection_string.startswith("duckdb://"):
    self.conn = ibis.duckdb.connect(
        connection_string.replace("duckdb://", ""),
        read_only=read_only
    )
# Support for other databases (PostgreSQL, etc.) is planned
```

## Executing Queries

The `read_query()` method is the primary interface for executing SQL queries:

```python
@lru_cache(maxsize=128)
def read_query(self, query: str) -> str:
    """
    Execute a read-only SQL query and return results as CSV
    """
    # Validation and execution implementation
```

This method:

1. Validates the SQL through `sql_validator.validate_sql()`
2. If validation passes, executes the query through Ibis
3. Applies row limits to prevent performance issues
4. Returns results in CSV format
5. Handles errors and exceptions

## Caching

Query results are cached using Python's `lru_cache` decorator to improve performance:

```python
@lru_cache(maxsize=128)
def read_query(self, query: str) -> str:
    # Implementation...
```

This caches up to 128 recent query results, avoiding redundant database calls.

## Information Schema Access

The `get_information_schema()` method provides metadata about tables and columns:

```python
@lru_cache(maxsize=128)
def get_information_schema(self) -> Dict[str, List[str]]:
    """Get the information schema of the database."""
    # Implementation...
```

This method returns table schema information as CSV, filtered according to security settings.

## Error Handling

When errors occur during execution, specific exception types are raised:

- `ExceptionGroup` for validation errors (containing detailed validation failure information)
- `QueryError` for execution failures
- `ConnectionError` for database connection issues

## Row Limiting

To prevent resource exhaustion, a row limit is applied to all queries:

```python
result = self.conn.sql(query).limit(self.row_limit)  # Default: 1000 rows
```

This limit can be configured during initialization.

## Integration with MCP Tools

The database functionality is exposed through MCP tools in `main.py`:

- `Get_Information_Schema` - Calls the `get_information_schema()` method
- `Select_Query` - Calls the `read_query()` method with user-provided SQL

## Usage Example

Here's an example of how to query an OMOP database using this system:

```python
# Initialize database connection
db = OmopDatabase(
    connection_string="duckdb:///path/to/omop.duckdb",
    read_only=True
)

# Execute a query and get results as CSV
try:
    results_csv = db.read_query("""
        SELECT p.person_id, p.year_of_birth, c.concept_name as gender
        FROM person p
        JOIN concept c ON p.gender_concept_id = c.concept_id
        LIMIT 10
    """)
    print(results_csv)
except Exception as e:
    print(f"Query failed: {e}")
```

!!! tip "Best Practice"
    Always use the JOIN syntax to resolve concept IDs to their human-readable names from the concept table rather than using source value columns directly.
