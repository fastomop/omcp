# SQL Validation

The SQL validation system ensures that queries sent to the OMOP database are safe, secure, and follow proper schema restrictions. The core of this system is the `SQLValidator` class in the `sql_validator.py` module.

## SQLValidator Class

The `SQLValidator` class performs multiple validation checks on SQL queries before they're executed against the database.

### Initialization

The validator can be configured with several parameters to control validation behavior:

```python
def __init__(
    self,
    allow_source_value_columns: bool = False,
    exclude_tables: t.List = None,
    exclude_columns: t.List = None,
    from_dialect: str = "postgres",
    to_dialect: str = "duckdb",
):
```

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `allow_source_value_columns` | bool | `False` | Whether to allow queries with source value columns |
| `exclude_tables` | List[str] | `None` | Tables that can't be queried |
| `exclude_columns` | List[str] | `None` | Columns that can't be queried |
| `from_dialect` | str | `"postgres"` | Source SQL dialect |
| `to_dialect` | str | `"duckdb"` | Target SQL dialect |

## Validation Process

When a query is submitted, the `validate_sql()` method performs several checks:

1. Parses the SQL using SQLGlot
2. Verifies it's a SELECT statement (not INSERT, UPDATE, etc.)
3. Extracts tables and columns referenced in the query
4. Validates tables against the allowed OMOP table list
5. Checks for excluded tables and columns
6. Inspects for source value columns if restricted

The validation process returns a list of errors, or an empty list if the query is valid.

## Validation Checks

### SELECT Statement Check

Only SELECT statements are allowed for security reasons:

```python
def _check_is_select_query(self, parsed_sql: exp.Expression) -> ex.NotSelectQueryError:
    if not isinstance(parsed_sql, exp.Select):
        return ex.NotSelectQueryError(
            "Only SELECT statements are allowed for security reasons."
        )
```

### OMOP Table Check

Ensures queries only reference valid OMOP CDM tables:

```python
def _check_is_omop_table(self, tables: t.List[exp.Table]) -> ex.TableNotFoundError:
    not_omop_tables = [
        table.name.lower()
        for table in tables
        if table.name.lower() not in OMOP_TABLES
    ]
    if not_omop_tables:
        return ex.TableNotFoundError(
            f"Tables not found in OMOP CDM: {', '.join(not_omop_tables)}"
        )
```

The validator maintains a list of valid OMOP tables:

```python
OMOP_TABLES = [
    "care_site",
    "cdm_source",
    "concept",
    # ... other OMOP tables
]
```

### Excluded Tables and Columns

The validator can be configured to block specific tables and columns:

```python
def _check_unauthorized_tables(self, tables: t.List[exp.Table]) -> ex.UnauthorizedTableError:
    unauthorized_tables = [
        table.name.lower()
        for table in tables
        if table.name.lower() in self.exclude_tables
    ]

    if unauthorized_tables:
        return ex.UnauthorizedTableError(
            f"Unauthorized tables in query: {', '.join(unauthorized_tables)}"
        )
```

### Source Value Columns

For privacy and security, source value columns can be restricted:

```python
def _check_source_value_columns(self, columns: t.List[exp.Column]) -> ex.UnauthorizedColumnError:
    if self.allow_source_value_columns:
        return None

    source_value_columns = [
        column.name.lower()
        for column in columns
        if column.name.lower().endswith("_source_value")
        or column.name.lower().endswith("_source_concept_id")
    ]

    if source_value_columns:
        return ex.UnauthorizedColumnError(
            f"Source value columns are not allowed: {', '.join(source_value_columns)}. "
            # ... additional message ...
        )
```

## Error Handling

The validator uses custom exception types from `omcp.exceptions` to provide clear error messages:

- `TableNotFoundError` - When tables don't exist in the OMOP schema
- `UnauthorizedTableError` - When queries use forbidden tables
- `UnauthorizedColumnError` - When queries use forbidden columns
- `NotSelectQueryError` - When non-SELECT statements are attempted

## SQLGlot Integration

The validator leverages [SQLGlot](https://sqlglot.com/) for SQL parsing and analysis, which provides:

- Robust SQL parsing across dialects
- AST (Abstract Syntax Tree) traversal
- Query component extraction

!!! note "Dialect Support"
    While the default configuration is set for PostgreSQL as the input dialect and DuckDB as the output dialect, this can be customized during initialization.
