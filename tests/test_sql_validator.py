import pytest
from omcp.sql_validator import SQLValidator
import omcp.exceptions as ex


@pytest.fixture
def validator():
    """Create a default SQL validator for testing"""
    return SQLValidator()


@pytest.fixture
def validator_with_source_values():
    """Create a SQL validator that allows source value columns"""
    return SQLValidator(allow_source_value_columns=True)


@pytest.fixture
def validator_with_table_exclusions():
    """Create a SQL validator with exclusions"""
    return SQLValidator(
        exclude_tables=["person", "observation"],
    )


@pytest.fixture
def validator_with_column_exclusions():
    """Create a SQL validator with column exclusions"""
    return SQLValidator(exclude_columns=["year_of_birth", "ethnicity_concept_id"])


@pytest.fixture
def validator_with_table_and_column_exclusions():
    """Create a SQL validator with table and column exclusions"""
    return SQLValidator(
        exclude_tables=["person", "observation"],
        exclude_columns=["year_of_birth", "ethnicity_concept_id"],
    )


class TestSQLValidator:
    def test_validate_select_statement(self, validator):
        """Test that a valid SELECT statement passes validation"""
        sql = (
            "SELECT person_id, gender_concept_id FROM person WHERE year_of_birth > 1970"
        )
        errors = validator.validate_sql(sql)
        assert len(errors) == 0, f"Expected no errors, got: {errors}"

    def test_non_select_statement(self, validator):
        """Test that non-SELECT statements are rejected"""
        sql = "INSERT INTO person (person_id) VALUES (1)"
        errors = validator.validate_sql(sql)
        assert len(errors) == 1, f"Expected 1 error, got: {errors}"
        assert isinstance(errors[0], ex.NotSelectQueryError)

    def test_non_omop_table(self, validator):
        """Test that non-OMOP tables are rejected"""
        sql = "SELECT id FROM users"
        errors = validator.validate_sql(sql)
        assert len(errors) == 1
        assert isinstance(errors[0], ex.TableNotFoundError)

    def test_unauthorized_table(self, validator_with_table_exclusions):
        """Test that excluded tables are rejected"""
        sql = "SELECT person_id FROM person"
        errors = validator_with_table_exclusions.validate_sql(sql)
        assert len(errors) == 1
        assert isinstance(errors[0], ex.UnauthorizedTableError)

    def test_unauthorized_column(self, validator_with_column_exclusions):
        """Test that excluded columns are rejected"""
        sql = "SELECT year_of_birth FROM person"
        errors = validator_with_column_exclusions.validate_sql(sql)
        assert len(errors) == 1
        assert isinstance(errors[0], ex.UnauthorizedColumnError)

    def test_unauthorized_table_and_column(
        self, validator_with_table_and_column_exclusions
    ):
        """Test that both excluded tables and columns are rejected"""
        sql = "SELECT year_of_birth FROM person"
        errors = validator_with_table_and_column_exclusions.validate_sql(sql)
        assert len(errors) == 2
        error_types = [type(e) for e in errors]
        assert ex.UnauthorizedTableError in error_types
        assert ex.UnauthorizedColumnError in error_types

    def test_source_value_columns_not_allowed(self, validator):
        """Test that source value columns are rejected by default"""
        sql = "SELECT person_id, gender_source_value FROM person"
        errors = validator.validate_sql(sql)
        assert len(errors) == 1
        assert isinstance(errors[0], ex.UnauthorizedColumnError)
        assert "Source value columns are not allowed" in str(errors[0])

    def test_source_value_columns_allowed(self, validator_with_source_values):
        """Test that source value columns are allowed when configured"""
        sql = "SELECT person_id, gender_source_value FROM person"
        errors = validator_with_source_values.validate_sql(sql)
        assert len(errors) == 0, f"Expected no errors, got: {errors}"

    # def test_sql_syntax_error(self, validator):
    #     """Test that SQL syntax errors are caught"""
    #     sql = "SELECT FROM person" # Missing column list
    #     errors = validator.validate_sql(sql)
    #     assert len(errors) == 1
    #     assert "ParseError" in str(type(errors[0]))

    def test_complex_query_validation(self, validator):
        """Test validation of a more complex query with joins"""
        sql = """
        SELECT p.person_id, p.gender_concept_id, c.concept_name
        FROM person p
        JOIN concept c ON p.gender_concept_id = c.concept_id
        WHERE p.year_of_birth > 1970
        """
        errors = validator.validate_sql(sql)
        assert len(errors) == 0, f"Expected no errors, got: {errors}"

    def test_source_concept_id_columns(self, validator):
        """Test that source_concept_id columns are rejected by default"""
        sql = "SELECT person_id, gender_source_concept_id FROM person"
        errors = validator.validate_sql(sql)
        assert len(errors) == 1
        assert isinstance(errors[0], ex.UnauthorizedColumnError)
        assert "Source value columns are not allowed" in str(errors[0])

    def test_check_is_omop_table_ignores_cte(self, validator):
        """Test that _check_is_omop_table ignores CTEs"""
        sql = """
        WITH patient AS (
            SELECT person_id, gender_concept_id FROM person
        ),
        visits AS (
            SELECT visit_occurrence_id, person_id FROM visit_occurrence
        )
        SELECT p.person_id, v.visit_occurrence_id
        FROM patient p
        JOIN visits v ON p.person_id = v.person_id
        """
        errors = validator.validate_sql(sql)
        assert len(errors) == 0, f"Expected no errors, got: {errors}"

    def test_check_is_omop_table_ignores_multiple_ctes(self, validator):
        """Test that _check_is_omop_table ignores multiple CTEs with non-OMOP tables"""
        sql = """
        WITH temp_users AS (
            SELECT person_id, year_of_birth, gender_concept_id
            FROM person
        ),
        temp_visits AS (
            SELECT visit_occurrence_id, person_id, visit_start_date
            FROM visit_occurrence
        )
        SELECT p.person_id, p.year_of_birth, v.visit_start_date
        FROM temp_users p
        JOIN temp_visits v ON p.person_id = v.person_id
        """

        errors = validator.validate_sql(sql)
        assert len(errors) == 0, f"Expected no errors, got: {errors}"
