import json
import logging.config
from pathlib import Path

from app.core.config import settings


def setup_logging():
    """Configure logging from the JSON config file"""
    try:
        logging_config_path = settings.CONFIG_DIR / "logging.json"

        if logging_config_path.exists():
            with open(logging_config_path, "r") as f:
                config = json.load(f)

            # Ensure log file path exists
            log_file = config.get("handlers", {}).get("file", {}).get("filename")
            if log_file:
                # Convert relative path to absolute
                if not Path(log_file).is_absolute():
                    config["handlers"]["file"]["filename"] = str(settings.LOG_DIR / log_file)

                # Ensure directory exists
                Path(config["handlers"]["file"]["filename"]).parent.mkdir(exist_ok=True)

            # Apply configuration
            logging.config.dictConfig(config)
        else:
            # Fallback configuration
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[
                    logging.StreamHandler(),
                    logging.FileHandler(settings.LOG_DIR / "mcp_server.log")
                ]
            )
            logging.warning(f"Logging config file not found at {logging_config_path}, using defaults")

    except Exception as e:
        # If all else fails
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(f"Error setting up logging: {e}")

    # Return the logger for the application
    return logging.getLogger("omop-mcp-server")


# Create logger instance
logger = setup_logging()