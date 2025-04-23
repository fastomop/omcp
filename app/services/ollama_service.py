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

                # Default confidence value
                confidence = 0.9

                # Return tuple directly
                return sql_query, confidence

        except Exception as e:
            logger.error(f"Error calling Ollama API: {e}")
            raise Exception(f"Failed to generate SQL: {str(e)}")

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