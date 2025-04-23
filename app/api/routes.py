from fastapi import APIRouter, Depends, HTTPException, status
from ..services.sql_service import SQLService
from ..services.ollama_service import OllamaService
from ..services.validation import ValidationService
from ..core.logging_setup import logger
from .models import Query, SQLResult, NaturalLanguageResponse, ErrorResponse

router = APIRouter(prefix="/api/v1")


@router.post("/query", response_model=NaturalLanguageResponse)
async def process_query(query: Query,
                        sql_service: SQLService = Depends(),
                        ollama_service: OllamaService = Depends(),
                        validation_service: ValidationService = Depends()):
    """Process a natural language query about the OMOP CDM database"""
    logger.info(f"Received query: {query.question}")

    try:
        # Generate SQL from natural language
        result = await ollama_service.generate_sql(query.question, query.context)
        logger.info(f"Result type: {type(result)}")
        sql_query, confidence = result

        # Validate the SQL
        is_valid, issues = validation_service.validate_query(sql_query)
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Generated SQL failed validation: {issues}"
            )

        # Execute the SQL
        result = await sql_service.execute_query(sql_query)
        results, execution_time = result

        # Generate natural language response
        answer = await ollama_service.generate_answer(query.question, sql_query, results)

        return NaturalLanguageResponse(
            answer=answer,
            sql=sql_query,
            confidence=confidence
        )

    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/sql", response_model=SQLResult)
async def execute_sql(query: Query,
                      sql_service: SQLService = Depends(),
                      ollama_service: OllamaService = Depends(),
                      validation_service: ValidationService = Depends()):
    """Generate and execute SQL from a natural language query"""
    logger.info(f"Generating SQL for: {query.question}")
    logger.info(f"Context: {query.context}")
    logger.info(f"OllamaService type: {type(ollama_service)}")
    logger.info(f"generate_sql type: {type(ollama_service.generate_sql)}")

    try:
        # Generate SQL from natural language
        result = await ollama_service.generate_sql(prompt=query.question, schema=query.context)
        logger.info(f"Result type: {type(result)}")
        sql_query, confidence = result
        # Validate the SQL
        is_valid, issues = validation_service.validate_query(sql_query)
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Generated SQL failed validation: {issues}"
            )

        # Execute the SQL
        result = await sql_service.execute_query(sql_query)
        results, execution_time = result

        return SQLResult(
            sql=sql_query,
            result=results,
            execution_time=execution_time
        )

    except Exception as e:
        logger.error(f"Error executing SQL: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )