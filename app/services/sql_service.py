from typing import Dict, Any, List, Tuple, Optional
from sqlalchemy import create_engine, text
import json
import duckdb

from app.core.config import settings
from app.core.logging_setup import logger


class SQLService:
    """Service for SQL database operations"""

    def __init__(self):
        # Dictionary to cache database engines
        self._db_engines = {}

    def get_db_engine(self, connection_id: Optional[str] = None, connection_string: Optional[str] = None):
        """Get or create a database engine"""
        if connection_string:
            # Create a temporary engine for one-time use
            return create_engine(connection_string)

        # Use a connection from the config
        conn_id = connection_id or "default"

        if conn_id not in self._db_engines:
            try:
                conn_string = settings.get_db_connection_string(conn_id)
                self._db_engines[conn_id] = create_engine(conn_string)
            except Exception as e:
                logger.error(f"Failed to create engine for {conn_id}: {e}")
                raise

        return self._db_engines[conn_id]

    async def execute_query(self, query: str, connection_id: Optional[str] = None,
                      connection_string: Optional[str] = None) -> Tuple[List[Dict[str, Any]], float]:
        """Execute a SQL query and return results and execution time"""
        import time
        start_time = time.time()

        try:
            engine = self.get_db_engine(connection_id, connection_string)

            with engine.connect() as connection:
                result = connection.execute(text(query))
                column_names = result.keys()

                # Convert rows to dictionaries
                rows = []
                for row in result:
                    rows.append({column_names[i]: row[i] for i in range(len(column_names))})

                execution_time = time.time() - start_time
                return rows, execution_time

        except Exception as e:
            logger.error(f"Database error: {e}")
            raise Exception(f"Database error: {str(e)}")

    def test_connection(self, connection_string: str) -> bool:
        """Test if a connection string is valid"""
        try:
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_omop_schema(self) -> str:
        """Load and format OMOP CDM schema for prompting"""
        try:
            schema_path = settings.get_omop_schema_path()

            with open(schema_path, "r") as f:
                schema_data = json.load(f)

            # Format the schema for the prompt
            schema_text = "OMOP CDM Database Schema:\n\n"

            # Add main tables first
            core_tables = ["person", "visit_occurrence", "condition_occurrence", "drug_exposure", "measurement",
                           "observation"]

            # First add core tables for better context
            schema_text += "Core Tables:\n"
            for table_name in core_tables:
                table = next((t for t in schema_data["tables"] if t["name"] == table_name), None)
                if table:
                    schema_text += f"Table: {table['name']} - {table.get('description', '')}\n"

                    for col in table["columns"]:
                        col_desc = f"  - {col['name']} ({col['data_type']})"
                        if col.get("description"):
                            col_desc += f": {col['description']}"
                        schema_text += col_desc + "\n"

                    schema_text += "\n"

            # Then add other tables
            schema_text += "Other Tables:\n"
            for table in schema_data["tables"]:
                if table["name"] not in core_tables:
                    schema_text += f"Table: {table['name']} - {table.get('description', '')}\n"

                    # Add only key columns for non-core tables to avoid overwhelming the model
                    key_columns = [c for c in table["columns"] if
                                   c.get("is_key", False) or "_id" in c["name"] or "concept_id" in c["name"]]
                    for col in key_columns:
                        col_desc = f"  - {col['name']} ({col['data_type']})"
                        if col.get("description"):
                            col_desc += f": {col['description']}"
                        schema_text += col_desc + "\n"

                    schema_text += f"  - plus {len(table['columns']) - len(key_columns)} more columns\n\n"

            # Add common relationships
            schema_text += "\nKey Relationships:\n"
            for relation in schema_data.get("relationships", []):
                schema_text += f"- {relation['source_table']}.{relation['source_column']} -> {relation['target_table']}.{relation['target_column']}\n"

            return schema_text

        except Exception as e:
            logger.error(f"Failed to load OMOP schema: {e}")
            return "OMOP CDM Schema unavailable"


# Create a global service instance
sql_service = SQLService()