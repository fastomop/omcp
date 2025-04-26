"""SQL Validator Module
This module provides functionality to validate SQL queries using SQLGlot
"""

import sqlglot as sg
import sqlglot.expressions as exp
import typing as t
import omcp.exceptions as ex

OMOP_TABLES = [
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
    "vocabulary",
]


class SQLValidator:
    def __init__(
        self,
        allow_source_value_columns: bool = False,
        exclude_tables: t.List = None,
        exclude_columns: t.List = None,
        from_dialect: str = "postgres",
        to_dialect: str = "duckdb",
    ):
        """
        Initialize the SQLValidator with a list of allowed tables.

        Args:
            allowed_tables (list): A list of allowed table names for validation.
            allow_source_values (bool): Flag to allow source values in validation.
            exclude_tables (list): A list of tables to exclude from validation.
            exclude_columns (list): A list of columns to exclude from validation.
        """

        self.allow_source_value_columns: bool = allow_source_value_columns
        self.exclude_tables: t.List = (
            list(map(str.lower, exclude_tables)) if exclude_tables is not None else []
        )
        self.exclude_columns: t.List = (
            list(map(str.lower, exclude_columns)) if exclude_columns is not None else []
        )

    def _check_is_select_query(
        self, parsed_sql: exp.Expression
    ) -> ex.NotSelectQueryError:
        """
        Check if the parsed SQL query is a SELECT statement.

        Args:
            parsed_sql (exp.Expression): The parsed SQL query.

        Returns:
            NotSelectQueryError: If the query is not a SELECT statement.
        """
        if not isinstance(parsed_sql, exp.Select):
            return ex.NotSelectQueryError(
                "Only SELECT statements are allowed for security reasons."
            )

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

    def _check_unauthorized_tables(
        self, tables: t.List[exp.Table]
    ) -> ex.UnauthorizedTableError:
        """
        Checks for unauthorized tables in the provided list of tables.

        Args:
            tables (List[exp.Table]): A list of table expressions to validate.

        Returns:
            UnauthorizedTableError: An error indicating the presence of unauthorized tables
            in the query, if any are found. Otherwise, returns None.
        """

        unauthorized_tables = [
            table.name.lower()
            for table in tables
            if table.name.lower() in self.exclude_tables
        ]

        if unauthorized_tables:
            return ex.UnauthorizedTableError(
                f"Unauthorized tables in query: {', '.join(unauthorized_tables)}"
            )

    def _check_unauthorized_columns(
        self, columns: t.List[exp.Column]
    ) -> ex.UnauthorizedColumnError:
        """
        Checks for unauthorized columns in the provided list of columns.

        Args:
            columns (List[exp.Column]): A list of column expressions.

        Returns:
            UnauthorizedColumnError: An error indicating the presence of unauthorized columns
            in the query, if any are found. Otherwise, returns None.
        """
        print(self.exclude_columns)
        unauthorized_columns = [
            column.name.lower()
            for column in columns
            if column.name.lower() in self.exclude_columns
        ]
        if unauthorized_columns:
            return ex.UnauthorizedColumnError(
                f"Unauthorized columns in query: {', '.join(unauthorized_columns)}"
            )

    def _check_source_value_columns(
        self, columns: t.List[exp.Column]
    ) -> ex.UnauthorizedColumnError:
        """
        Check if the query contains source value or source_concept_id columns.

        Args:
            columns (list): A list of column expressions.

        Returns:
            UnauthorizedColumnError: If source value columns are found.
        """

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
                f"Use the corresponding concept_id columns with a join on the concept table instead. "
                f"Inform the user that this is a security measure to prevent data leakage."
            )

    def validate_sql(self, sql: str):
        """
        Validate the SQL query.

        Args:
            sql (str): The SQL query to validate.

        Returns:
            list: A list of errors found during validation. If no errors, returns an empty list.

        """

        errors = []

        try:
            # Parse the SQL query
            parsed_sql = sg.parse_one(sql)

            # Validate the query to ensure it's a SELECT statement

            is_not_select_query = self._check_is_select_query(parsed_sql)
            if is_not_select_query:
                raise is_not_select_query

            tables = list(parsed_sql.find_all(exp.Table))
            columns = list(parsed_sql.find_all(exp.Column))
            # joins = parsed_sql.find_all(exp.Join)
            # where_clauses = parsed_sql.find_all(exp.Where)

            if not tables:
                errors.append(ex.TableNotFoundError("No tables found in the query."))
            if not columns:
                errors.append(ex.ColumnNotFoundError("No columns found in the query."))

            # Check is OMOP table
            errors.append(self._check_is_omop_table(tables))

            # Check for excluded tables
            errors.append(self._check_unauthorized_tables(tables))

            # Check for excluded columns
            errors.append(self._check_unauthorized_columns(columns))

            # Check for source value columns
            errors.append(self._check_source_value_columns(columns))

        except sg.ParseError as e:
            errors.append(e)
        except Exception as e:
            errors.append(e)
        finally:
            errors = list(filter(None, errors))  # Remove None values from the list
            return errors
