from typing import Dict, Any, Tuple
import re
import json

from app.core.config import settings
from app.core.logging_setup import logger
from app.api.models import ValidationResult
from app.services.agent_service import agent_service  # Use the existing agent service


class ValidationService:
    """Service for validating SQL queries against OMOP CDM rules"""

    def __init__(self):
        self.validation_rules = self._load_validation_rules()
        self.agent_type = "medical_validator"  # The agent type from your config

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

    async def validate_query(self, sql_query: str) -> Tuple[bool, list]:
        """Simplified validation method for debugging"""
        try:
            logger.info(f"Validating SQL query: {sql_query}")

            # Just do local validation for now
            validation = self._local_validation(sql_query)
            logger.info(f"Local validation result: is_valid={validation.is_valid}, issues={validation.issues}")

            return validation.is_valid, validation.issues

        except Exception as e:
            logger.error(f"Error in validate_query: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise Exception(f"Validation failed: {str(e)}")

    async def _agent_validation(self, sql_query: str) -> Tuple[bool, list]:
        """Call the external validator agent for SQL syntax validation using agent_service"""
        try:
            # Check if validator agent is configured
            available_agents = agent_service.get_available_agents()

            if self.agent_type not in available_agents:
                logger.warning(f"Validator agent '{self.agent_type}' not configured, skipping agent validation")
                return True, []  # Skip agent validation if not configured

            from app.api.models import AgentIntegration
            # Create agent integration request
            agent_request = AgentIntegration(
                agent_type=self.agent_type,
                context={"query": sql_query}
            )

            # Call agent service
            logger.info(f"Calling validator agent: {self.agent_type}")
            result = await agent_service.get_agent_insights("", sql_query, agent_request)

            # Check for errors
            if "error" in result:
                logger.warning(f"Validator agent error: {result['error']}")
                return True, [f"Warning: {result['error']}"]

            # Process response
            is_valid = result.get("is_valid", False)
            issues = result.get("issues", [])

            return is_valid, issues

        except Exception as e:
            logger.error(f"Validator agent error: {e}")
            return True, [f"Warning: SQL validation error: {str(e)}"]

    def _local_validation(self, sql_query: str) -> ValidationResult:
        """Perform local rule-based validation"""
        # This method can remain the same as in your current implementation
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

        logger.info(f"Local validation completed: {validation}")

        return validation

    async def refine_with_llm(self, sql_query: str, issues: list) -> Tuple[bool, str, list]:
        """Use the LLM to refine a SQL query based on validation issues"""
        from app.services.ollama_service import ollama_service

        try:
            # Create a prompt for the LLM
            issues_text = "\n".join([f"- {issue}" for issue in issues])

            # Get the OMOP schema for context
            schema = ""
            try:
                from app.services.sql_service import sql_service
                schema = sql_service.get_omop_schema()
            except Exception as e:
                logger.warning(f"Could not get OMOP schema for refinement: {e}")

            prompt = f"""
            You are a SQL expert specializing in OMOP CDM. 

            Here is the OMOP CDM schema summary:
            {schema}

            The following SQL query has validation issues:

            ```sql
            {sql_query}
            ```

            Validation issues:
            {issues_text}

            Please correct the SQL query to fix these issues. The query should be valid SQL that follows OMOP CDM best practices.

            Return ONLY the corrected SQL query without any explanations or markdown.
            """

            # Use the Ollama service to get a refined SQL
            logger.info(f"Attempting to refine SQL query with LLM")
            refined_sql, _ = await ollama_service.generate_sql(prompt, schema)

            # Clean up the refined SQL (remove any explanatory text)
            refined_sql = self._extract_sql_from_text(refined_sql)

            logger.info(f"Original SQL: {sql_query}")
            logger.info(f"Refined SQL: {refined_sql}")

            # Validate the refined SQL
            validation = self._local_validation(refined_sql)
            if not validation.is_valid:
                logger.warning(f"Refined SQL still has local validation issues: {validation.issues}")
                return False, refined_sql, validation.issues

            # If local validation passes, check with the agent
            agent_valid, agent_issues = await self._agent_validation(refined_sql)
            if not agent_valid:
                logger.warning(f"Refined SQL still has agent validation issues: {agent_issues}")
                return False, refined_sql, agent_issues

            logger.info(f"SQL refinement successful")
            return True, refined_sql, []
        except Exception as e:
            logger.error(f"Error refining SQL query with LLM: {e}")
            return False, sql_query, [f"LLM refinement error: {str(e)}"] + issues

    def _extract_sql_from_text(self, text: str) -> str:
        """Extract SQL query from text that might contain explanations or markdown"""
        # Try to extract SQL from markdown code blocks
        sql_block_pattern = r"```sql\s+(.*?)\s+```"
        matches = re.findall(sql_block_pattern, text, re.DOTALL)
        if matches:
            return matches[0].strip()

        # Try to extract any code block
        code_block_pattern = r"```\s+(.*?)\s+```"
        matches = re.findall(code_block_pattern, text, re.DOTALL)
        if matches:
            return matches[0].strip()

        # If no code blocks found, return the entire text
        return text.strip()

    async def validate_and_refine_query(self, sql_query: str) -> Dict[str, Any]:
        """Validate a SQL query and attempt refinement if needed"""
        try:
            logger.info(f"Starting validation for SQL: {sql_query}")

            # First, perform validation
            logger.info("Calling validate_query method")
            is_valid, issues = await self.validate_query(sql_query)
            logger.info(f"Validation result: is_valid={is_valid}, issues={issues}")

            result = {
                "is_valid": is_valid,
                "original_sql": sql_query,
                "issues": issues,
                "refinement_attempted": False,
                "refinement_successful": False,
                "refined_sql": None,
                "refined_issues": None
            }

            # If validation passed, no refinement needed
            if is_valid:
                logger.info("Validation passed, no refinement needed")
                return result

            # Attempt refinement if validation failed
            logger.info(f"Validation failed with issues: {issues}. Attempting refinement.")
            result["refinement_attempted"] = True

            logger.info("Calling refine_with_llm method")
            refinement_successful, refined_sql, remaining_issues = await self.refine_with_llm(sql_query, issues)
            logger.info(
                f"Refinement result: successful={refinement_successful}, refined_sql={refined_sql}, issues={remaining_issues}")

            result["refinement_successful"] = refinement_successful
            result["refined_sql"] = refined_sql
            result["refined_issues"] = remaining_issues

            # Update overall validity if refinement was successful
            if refinement_successful:
                result["is_valid"] = True
                result["issues"] = []  # Clear issues since refinement fixed them

            logger.info(f"Final validation and refinement result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error in validate_and_refine_query: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise


# Create a global service instance
validation_service = ValidationService()