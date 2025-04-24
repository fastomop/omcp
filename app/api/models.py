import os, json
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union


class DatabaseConnection(BaseModel):
    connection_id: str = Field(..., description="ID for the database connection to use")
    connection_string: Optional[str] = Field(None, description="Direct connection string if not using predefined")


class ModelSelection(BaseModel):
    model_name: str = Field(..., description="Name of the Ollama model to use")
    temperature: Optional[float] = Field(0.1, description="Temperature for model generation")
    top_p: Optional[float] = Field(0.9, description="Top-p for sampling")


class AgentIntegration(BaseModel):
    agent_url: Optional[str] = Field(None, description="URL of external agent to consult")
    agent_type: str = Field("medical_expert", description="Type of agent to integrate")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context for the agent")


class MCPRequest(BaseModel):
    prompt: str = Field(..., description="Natural language query to convert to SQL")
    system_prompt: Optional[str] = Field(None, description="Custom system prompt")
    database: Optional[DatabaseConnection] = Field(None, description="Database connection details")
    model: Optional[ModelSelection] = Field(None, description="Model selection details")
    execute_query: Optional[bool] = Field(False, description="Whether to execute the generated query")
    validate_against_omop: Optional[bool] = Field(True, description="Whether to validate against OMOP CDM")
    agent_integration: Optional[AgentIntegration] = Field(None, description="External agent integration details")


class ValidationResult(BaseModel):
    is_valid: bool
    issues: Optional[List[str]] = None


class MCPResponse(BaseModel):
    sql: str
    explanation: Optional[str] = None
    results: Optional[List[Dict[str, Any]]] = None
    validation: Optional[ValidationResult] = None
    error: Optional[str] = None
    agent_insights: Optional[Dict[str, Any]] = None


class HealthCheckResponse(BaseModel):
    status: str
    default_model: str
    available_databases: List[str]


class DatabaseConnectionRequest(BaseModel):
    connection_id: str = Field(..., description="Identifier for the connection")
    connection_string: str = Field(..., description="Database connection string (e.g., sqlite:///data/db.sqlite)")


class DatabaseConnectionResponse(BaseModel):
    message: str
    connection_id: str

def get_default_schema():
    """Load the OMOP CDM schema as the default context"""
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                              "schemas", "omop_cdm_schema.json")
    try:
        with open(schema_path, "r") as f:
            return json.dumps(json.load(f))
    except Exception as e:
        # Fallback for error cases
        return None

class Query(BaseModel):
    question: str
    context: Optional[str] = Field(default_factory=get_default_schema)

class NaturalLanguageResponse(BaseModel):
    answer: str
    sql: str
    confidence: float

class ErrorResponse(BaseModel):
    detail: str

class SQLValidationRequest(BaseModel):
    sql: str = Field(..., description="SQL query to validate")

class SQLValidationResponse(BaseModel):
    is_valid: bool
    issues: Optional[List[str]] = None

class ValidationResult(BaseModel):
    is_valid: bool
    issues: Optional[List[str]] = None

class RefinementInfo(BaseModel):
    original_sql: str
    was_refined: bool

class NaturalLanguageResponse(BaseModel):
    answer: str
    sql: str
    confidence: float
    refinement_info: Optional[RefinementInfo] = None

class SQLResult(BaseModel):
    sql: str
    result: Any
    execution_time: float
    refinement_info: Optional[RefinementInfo] = None
