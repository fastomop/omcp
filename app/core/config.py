import json
import os
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """Application settings"""

    def __init__(self):

        self.PROJECT_NAME = "OMCP Server"
        self.VERSION = "1.0.0"

        # Server settings
        self.HOST = os.getenv("HOST", "0.0.0.0")
        self.PORT = int(os.getenv("PORT", "8000"))
        self.DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")

        # Base paths
        self.BASE_DIR = Path(__file__).resolve().parent.parent.parent
        self.CONFIG_DIR = self.BASE_DIR / "config"
        self.SCHEMA_DIR = self.BASE_DIR / "schemas"
        self.LOG_DIR = self.BASE_DIR / "logs"

        # Ensure directories exist
        self.LOG_DIR.mkdir(exist_ok=True)

        # Load configuration from JSON
        self.config = self._load_json_config()

        # Override config with environment variables if provided
        if os.getenv("OLLAMA_API_URL"):
            self.config["ollama"]["api_url"] = os.getenv("OLLAMA_API_URL")

        if os.getenv("DEFAULT_MODEL"):
            self.config["ollama"]["default_model"] = os.getenv("DEFAULT_MODEL")

        if os.getenv("API_KEY_REQUIRED"):
            self.config["auth"]["require_api_key"] = os.getenv("API_KEY_REQUIRED").lower() in ("true", "1", "t")

        if os.getenv("DEFAULT_API_KEY"):
            # Add to existing keys, don't overwrite
            if os.getenv("DEFAULT_API_KEY") not in self.config["auth"]["api_keys"]:
                self.config["auth"]["api_keys"].append(os.getenv("DEFAULT_API_KEY"))

    def _load_json_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        config_path = self.CONFIG_DIR / "config.json"
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            # Default configuration if file not found or invalid
            print(f"Warning: Could not load config file: {e}")
            return {
                "server": {"host": "0.0.0.0", "port": 8000},
                "ollama": {"api_url": "http://localhost:11434/api/generate", "default_model": "codellama"},
                "database": {
                    "connection_strings": {"default": "sqlite:///data/example.db"},
                    "schema_directory": "schemas/"
                },
                "omop_cdm": {"validation_rules": "schemas/omop_validation_rules.json"},
                "auth": {"require_api_key": False, "api_keys": ["dev_key_12345"]},
                "agents": {}
            }

    def get_omop_schema_path(self) -> Path:
        """Get the path to the OMOP CDM schema file"""
        return self.SCHEMA_DIR / "omop_cdm_schema.json"

    def get_validation_rules_path(self) -> Path:
        """Get the path to the validation rules file"""
        return self.SCHEMA_DIR / self.config["omop_cdm"]["validation_rules"]

    def get_db_connection_string(self, connection_id: Optional[str] = None) -> str:
        """Get a database connection string by ID"""
        conn_id = connection_id or "default"
        if conn_id not in self.config["database"]["connection_strings"]:
            raise ValueError(f"Unknown connection ID: {conn_id}")
        return self.config["database"]["connection_strings"][conn_id]

    def get_agent_config(self, agent_type: str) -> Dict[str, Any]:
        """Get configuration for an agent type"""
        if agent_type not in self.config.get("agents", {}):
            raise ValueError(f"Unknown agent type: {agent_type}")
        return self.config["agents"][agent_type]


# Create a global settings instance
settings = Settings()