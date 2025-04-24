import requests
import json
from typing import Dict, Any, Optional, List, Tuple

from app.core.config import settings
from app.core.logging_setup import logger


class OllamaService:
    """Service for interacting with Ollama LLM API"""

    def __init__(self):
        self.api_url = settings.config["ollama"]["api_url"]
        self.default_model = settings.config["ollama"]["default_model"]

    async def generate_sql(self, prompt: str, schema: str,
                           model_name: Optional[str] = None,
                           system_prompt: Optional[str] = None,
                           model_options: Optional[Dict[str, Any]] = None) -> Tuple[str, float]:
        """Generate SQL from natural language using Ollama"""

        # Set up the model name and options
        model = model_name or self.default_model
        options = model_options or {}

        # Default system prompt for SQL generation
        default_system = "You are an expert in SQL and healthcare data analysis, specifically working with the OMOP Common Data Model (CDM)."
        system_message = system_prompt or default_system

        # Build the prompt for SQL generation
        full_prompt = f"""
    Given the following OMOP CDM schema:

    {schema}

    Convert the following natural language query into a valid SQL query that follows OMOP CDM best practices:

    "{prompt}"

    Return ONLY the SQL query without any additional text, explanations, or markdown formatting.
    Follow these important guidelines for OMOP CDM:
    1. Always join person table when querying patient-level data
    2. Always join concept tables when filtering by medical concepts
    3. Use appropriate date ranges for temporal queries
    4. Remember that most clinical data is in condition_occurrence, drug_exposure, measurement, and observation tables
    5. Make sure to handle NULL values appropriately
    6. Use concept_id for filtering and joining with the concept table
    7. Don't include any unnecessary complexity or additional features in the SQL query
    """

        # Prepare the request for Ollama
        ollama_request = {
            "model": model,
            "prompt": full_prompt,
            "system": system_message,
            "stream": False,
            **options
        }

        logger.debug(f"Sending request to Ollama model {model}")

        try:
            # Use httpx for async HTTP requests
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.post(self.api_url, json=ollama_request, timeout=240)
                response.raise_for_status()

                # Extract the generated response
                result = response.json()
                sql_query = result["response"].strip()

                # Clean up the SQL (in case it's wrapped in markdown code blocks)
                if sql_query.startswith("```") and sql_query.endswith("```"):
                    # Extract SQL from markdown code block
                    sql_query = sql_query.split("```")[1]
                    if sql_query.startswith("sql"):
                        sql_query = sql_query[3:].strip()

                # Extract SQL from response
                sql_query = result["response"].strip()

                # Clean up the SQL (in case it's wrapped in markdown code blocks or contains explanations)
                sql_query = self._extract_sql_from_text(sql_query)

                # Default confidence value
                confidence = 0.9

                # Return tuple directly
                return sql_query, confidence

        except Exception as e:
            logger.error(f"Error calling Ollama API: {e}")
            raise Exception(f"Failed to generate SQL: {str(e)}")

    def _extract_sql_from_text(self, text: str) -> str:
        """Extract SQL query from text that might contain explanations or markdown"""
        import re

        # Try to extract SQL from markdown code blocks with sql tag
        sql_block_pattern = r"```sql\s+(.*?)\s+```"
        matches = re.findall(sql_block_pattern, text, re.DOTALL)
        if matches:
            return matches[0].strip()

        # Try to extract from any markdown code block
        code_block_pattern = r"```\s+(.*?)\s+```"
        matches = re.findall(code_block_pattern, text, re.DOTALL)
        if matches:
            return matches[0].strip()

        # If no code blocks found but text contains SELECT, try to find the SQL statement
        if "SELECT" in text.upper():
            lines = text.split("\n")
            sql_lines = []
            in_sql = False

            for line in lines:
                if "SELECT" in line.upper() and not in_sql:
                    in_sql = True
                    sql_lines.append(line)
                elif in_sql:
                    sql_lines.append(line)
                    if ";" in line:  # End of SQL statement
                        break

            if sql_lines:
                return "\n".join(sql_lines)

        # If all extraction methods fail, assume the entire text is SQL
        # But check if it looks like SQL (contains SELECT)
        if "SELECT" in text.upper():
            return text

        # Final fallback - return a simple safe query
        logger.warning(f"Could not extract SQL from text: {text}")
        return "SELECT 1 AS dummy"

    async def refine_sql_with_omop_knowledge(self, sql_query: str, validation_issues: list,
                                             model_name: Optional[str] = None) -> Tuple[str, float]:
        """
        Refine a SQL query that failed OMOP CDM validation by using LLM with OMOP domain knowledge

        Args:
            sql_query: The original SQL query that failed validation
            validation_issues: List of validation issues identified
            model_name: Optional model name to use for refinement

        Returns:
            Tuple of (refined_sql, confidence_score)
        """
        try:
            model = model_name or self.default_model

            # Create a comprehensive prompt with OMOP knowledge
            issues_text = "\n".join([f"- {issue}" for issue in validation_issues])

            # Add specific OMOP CDM guidance
            omop_guidance = """
            OMOP CDM SQL Guidelines:
            1. Use person.gender_concept_id (8507=male, 8532=female) instead of a 'gender' column
            2. Always JOIN tables before referencing their columns (person, condition_occurrence, etc.)
            3. For conditions like diabetes, join condition_occurrence and use condition_concept_id
            4. For medications, join drug_exposure and use drug_concept_id
            5. For measurements, join measurement and use measurement_concept_id
            6. Always include appropriate person_id joins between clinical tables
            7. Use concept table to translate between concept names and IDs
            8. Include proper date filters on temporal data (using BETWEEN, >, <, etc.)
            9. Remember that most clinical data is in condition_occurrence, drug_exposure, measurement, and observation tables
            10. Use proper table aliases if needed, e.g., 'p' for person, 'co' for condition_occurrence
            """

            prompt = f"""
            You are an OMOP CDM SQL expert. The following SQL query has validation issues:

            ```sql
            {sql_query}
            ```

            Validation issues:
            {issues_text}

            {omop_guidance}

            Please fix the SQL query to resolve these issues. The query should be valid SQL that follows OMOP CDM best practices.

            Return ONLY the corrected SQL query without any explanations or markdown.
            """

            # Use system prompt to reinforce OMOP expertise
            system_prompt = """You are an expert in SQL and healthcare data analysis, specifically working with the OMOP Common Data Model (CDM).
            Your task is to fix SQL queries that don't comply with OMOP CDM standards. Return ONLY the fixed SQL query without any explanation."""

            # Call the LLM
            logger.info(f"Attempting to refine SQL query with LLM using model {model}")

            # Set options for more deterministic output
            options = {
                "temperature": 0.1,  ## Achtung: Low temperature for more deterministic output
                "top_p": 0.9,
                "max_tokens": 1000
            }

            # Use the Ollama API
            ollama_request = {
                "model": model,
                "prompt": prompt,
                "system": system_prompt,
                "stream": False,
                **options
            }

            # Use async HTTP for the request
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.post(self.api_url, json=ollama_request, timeout=240)
                response.raise_for_status()

                # Extract the generated response
                result = response.json()
                refined_sql = result["response"].strip()

                # Extract SQL from the response (remove any markdown or explanations)
                refined_sql = self._extract_sql_from_text(refined_sql)

                # Fix common OMOP SQL errors (if you've implemented this method)
                if hasattr(self, 'fix_common_omop_sql_errors'):
                    refined_sql = self.fix_common_omop_sql_errors(refined_sql)

                logger.info(f"Original SQL: {sql_query}")
                logger.info(f"Refined SQL: {refined_sql}")

                # Default confidence value
                confidence = 0.8

                return refined_sql, confidence

        except Exception as e:
            logger.error(f"Error refining SQL query with LLM: {e}")
            # Return the original query with low confidence in case of errors
            return sql_query, 0.1

    async def generate_explanation(self, sql_query: str,
                             model_name: Optional[str] = None) -> str:
        """Generate an explanation for the SQL query"""

        model = model_name or self.default_model

        explanation_prompt = f"""
Explain what this SQL query does in simple terms, focusing on the healthcare insights it provides:

```sql
{sql_query}
```

Explain in 2-3 sentences what clinical question this query answers and how it uses the OMOP CDM structure.
"""

        explanation_request = {
            "model": model,
            "prompt": explanation_prompt,
            "system": "You are a healthcare analytics expert explaining SQL queries to clinicians.",
            "stream": False
        }

        try:
            response = requests.post(self.api_url, json=explanation_request, timeout=120)
            response.raise_for_status()
            return response.json()["response"].strip()
        except requests.RequestException as e:
            logger.error(f"Error generating explanation: {e}")
            return "Explanation not available."

    def list_available_models(self) -> List[Dict[str, Any]]:
        """List available models from Ollama"""
        try:
            response = requests.get("http://localhost:11434/api/tags", timeout=10)
            response.raise_for_status()
            return response.json().get("models", [])
        except requests.RequestException as e:
            logger.error(f"Error listing models: {e}")
            return []

    async def generate_answer(self, question: str, sql_query: str, results: Any) -> str:
        """Generate a natural language answer based on the query, SQL, and results"""

        # Format results for the prompt
        results_str = json.dumps(results, default=str, indent=2)

        answer_prompt = f"""
        Given the following:

        Question: "{question}"

        SQL Query:
        ```sql
        {sql_query}
        ```

        Query Results:
        ```json
        {results_str}
        ```

        Generate a comprehensive natural language answer to the original question based on these results.
        Explain the insights from the data in a way that would be understandable to healthcare professionals.
        """

        answer_request = {
            "model": self.default_model,
            "prompt": answer_prompt,
            "system": "You are a healthcare analytics expert explaining query results to clinicians.",
            "stream": False
        }

        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.post(self.api_url, json=answer_request, timeout=240)
                response.raise_for_status()
                return response.json()["response"].strip()
        except Exception as e:
            logger.error(f"Error generating answer: {e}")
            return f"Error generating explanation: {str(e)}"
# Create a global service instance
ollama_service = OllamaService()