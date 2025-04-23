import json
import requests
from typing import Dict, Any, Tuple
import re

from app.core.config import settings
from app.core.logging_setup import logger
from app.api.models import ValidationResult


class ValidationService:
    """Service for validating SQL queries against OMOP CDM rules"""

    def __init__(self):
        self.validation_rules = self._load_validation_rules()
        self.validator_url = settings.agents["medical_validator"]["url"]
        self.timeout = settings.agents["medical_validator"]["timeout"]

    def _load_validation_rules(self) -> Dict[str, Any]:
        """Load validation rules from file"""
        try:
            rules_path = settings.get_validation_rules_path()
            with open(rules_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load validation rules: {e}")
            return {
                "required_tables": [],
                "required_joins": [],
                "concept_tables": []
            }

    def validate_query(self, sql_query: str) -> Tuple[bool, list]:
        """Validate a SQL query against OMOP CDM rules and using the validator agent"""
        # First, perform local validation of domain-specific rules
        validation = self._local_validation(sql_query)

        # If local validation fails, no need to call the agent
        if not validation.is_valid:
            return validation.is_valid, validation.issues

        # Then, perform syntax validation with the validator agent
        agent_valid, agent_issues = self._agent_validation(sql_query)
        if not agent_valid:
            validation.is_valid = False
            validation.issues.extend(agent_issues)

        return validation.is_valid, validation.issues

    def _agent_validation(self, sql_query: str) -> Tuple[bool, list]:
        """Call the external validator agent for SQL syntax validation"""
        try:
            response = requests.post(
                self.validator_url,
                json={"query": sql_query},
                timeout=self.timeout
            )
            response.raise_for_status()
            result = response.json()
            return result.get("is_valid", False), result.get("issues", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Validator agent request error: {e}")
            return False, [f"SQL syntax validation failed: {str(e)}"]
        except Exception as e:
            logger.error(f"Validator agent error: {e}")
            return False, [f"SQL validation error: {str(e)}"]

    def _local_validation(self, sql_query: str) -> ValidationResult:
        """Perform local rule-based validation"""
        # Initialize validation result
        validation = ValidationResult(is_valid=True, issues=[])

        # Convert SQL to lowercase for case-insensitive checks
        sql_lower = sql_query.lower()

        # Check for prohibited operations
        prohibited_patterns = [
            r'\bdrop\s+table\b',
            r'\btruncate\s+table\b',
            r'\bdelete\s+from\b',
            r'\bupdate\s+\w+\s+set\b',
            r'\balter\s+table\b'
        ]

        for pattern in prohibited_patterns:
            if re.search(pattern, sql_lower):
                validation.is_valid = False
                validation.issues.append("Query contains prohibited operations (DROP, TRUNCATE, DELETE, UPDATE, ALTER)")
                break

        # Check for required tables
        required_tables = self.validation_rules.get("required_tables", [])
        for table in required_tables:
            trigger = table["when"].lower()
            table_name = table["name"].lower()

            # Check if the trigger word is in the query but the required table is not
            if trigger in sql_lower and table_name not in sql_lower:
                validation.is_valid = False
                validation.issues.append(f"Missing required table {table['name']} when querying {table['when']}")

        # Check for required joins
        required_joins = self.validation_rules.get("required_joins", [])
        for join in required_joins:
            table1 = join["table1"].lower()
            table2 = join["table2"].lower()
            condition = join["condition"].lower()

            # If both tables are in the query, check for the join condition
            if table1 in sql_lower and table2 in sql_lower:
                # Look for different variants of the join condition
                condition_variants = [
                    condition,
                    condition.replace(" = ", "="),
                    condition.replace("=", " = "),
                    # Add other common variants
                ]

                if not any(variant in sql_lower for variant in condition_variants):
                    validation.is_valid = False
                    validation.issues.append(
                        f"Missing proper join condition between {join['table1']} and {join['table2']}")

        # Check for concept_id filters when using concept tables
        concept_tables = self.validation_rules.get("concept_tables", [])
        for concept_table in concept_tables:
            table_name = concept_table.lower()
            if table_name in sql_lower and "concept_id" not in sql_lower:
                validation.issues.append(f"Warning: Querying {concept_table} without concept_id filter")

        # Check for date range filters on temporal queries
        if any(term in sql_lower for term in ["date", "datetime", "time"]):
            if not any(term in sql_lower for term in ["between", ">", "<", ">=", "<="]):
                validation.issues.append("Warning: Temporal query without date range filter")

        # Check for basic SQL syntax issues (unbalanced parentheses, missing quotes)
        if sql_query.count('(') != sql_query.count(')'):
            validation.is_valid = False
            validation.issues.append("SQL syntax error: Unbalanced parentheses")

        # Check for unclosed quotes
        quote_chars = ["'", '"']
        for quote in quote_chars:
            if sql_query.count(quote) % 2 != 0:
                validation.is_valid = False
                validation.issues.append(f"SQL syntax error: Unclosed {quote} quotes")

        return validation


# Create a global service instance
validation_service = ValidationService()