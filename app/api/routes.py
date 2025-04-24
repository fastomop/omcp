from fastapi import APIRouter, Depends, HTTPException, status
from ..services.sql_service import SQLService
from ..services.ollama_service import OllamaService
from ..services.validation import ValidationService
from ..core.logging_setup import logger
from .models import Query, SQLResult, NaturalLanguageResponse, ErrorResponse, ValidationResult, MCPResponse, HealthCheckResponse, DatabaseConnectionRequest, DatabaseConnectionResponse

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

        # Validate and possibly refine the SQL
        validation_result = await validation_service.validate_and_refine_query(sql_query)

        # Use refined SQL if refinement was successful
        if validation_result["refinement_successful"]:
            logger.info(f"Using refined SQL: {validation_result['refined_sql']}")
            sql_query = validation_result["refined_sql"]
        elif not validation_result["is_valid"]:
            # If validation failed and refinement failed or wasn't attempted
            error_detail = {
                "message": "Generated SQL failed validation",
                "issues": validation_result["issues"],
                "refinement_attempted": validation_result["refinement_attempted"],
                "refined_sql": validation_result["refined_sql"],
                "refined_issues": validation_result["refined_issues"]
            }
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_detail
            )

        # Execute the SQL
        result = await sql_service.execute_query(sql_query)
        results, execution_time = result

        # Generate natural language response
        answer = await ollama_service.generate_answer(query.question, sql_query, results)

        return NaturalLanguageResponse(
            answer=answer,
            sql=sql_query,
            confidence=confidence,
            refinement_info={
                "original_sql": validation_result["original_sql"],
                "was_refined": validation_result["refinement_attempted"] and validation_result["refinement_successful"]
            }
        )

    except HTTPException:
        # Re-raise HTTP exceptions without modification
        raise
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

    try:
        # Generate SQL from natural language
        result = await ollama_service.generate_sql(prompt=query.question, schema=query.context)
        logger.info(f"Result type: {type(result)}")
        sql_query, confidence = result

        # Validate and possibly refine the SQL
        validation_result = await validation_service.validate_and_refine_query(sql_query)

        # Use refined SQL if refinement was successful
        if validation_result["refinement_successful"]:
            logger.info(f"Using refined SQL: {validation_result['refined_sql']}")
            sql_query = validation_result["refined_sql"]
        elif not validation_result["is_valid"]:
            # If validation failed and refinement failed or wasn't attempted
            error_detail = {
                "message": "Generated SQL failed validation",
                "issues": validation_result["issues"],
                "refinement_attempted": validation_result["refinement_attempted"],
                "refined_sql": validation_result["refined_sql"],
                "refined_issues": validation_result["refined_issues"]
            }
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_detail
            )

        # Execute the SQL
        result = await sql_service.execute_query(sql_query)
        results, execution_time = result

        refinement_info = None
        if "original_sql" in validation_result:
            refinement_info = {
                "original_sql": validation_result["original_sql"],
                "was_refined": validation_result["refinement_attempted"] and validation_result["refinement_successful"]
            }

        return SQLResult(
            sql=sql_query,
            result=results,
            execution_time=execution_time,
            refinement_info=refinement_info
        )

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error executing SQL: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/validate", response_model=ValidationResult)
async def validate_and_refine_sql(query: Query,
                                  validation_service: ValidationService = Depends()):
    """Validate SQL and attempt refinement if needed"""
    logger.info(f"Validating SQL: {query.question}")

    try:
        # If context contains SQL, use that, otherwise assume question is the SQL
        sql_query = query.context if query.context and query.context.strip().lower().startswith(
            "select") else query.question

        # Perform validation and refinement
        try:
            validation_result = await validation_service.validate_and_refine_query(sql_query)
            # Rest of your code
        except Exception as e:
            logger.error(f"Error during validation and refinement: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Validation error: {str(e) or 'Unknown error occurred during validation'}"
            )

        return ValidationResult(
            is_valid=validation_result["is_valid"],
            issues=validation_result["issues"],
            refinement_attempted=validation_result["refinement_attempted"],
            refinement_successful=validation_result["refinement_successful"],
            refined_sql=validation_result["refined_sql"],
            refined_issues=validation_result["refined_issues"]
        )
    except Exception as e:
        logger.error(f"Error validating SQL: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
