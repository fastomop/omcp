import os
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv
from langfuse import Langfuse
from langfuse import observe

load_dotenv()

# Global configuration variables
ENABLE_LOGGING = os.environ.get('ENABLE_LOGGING', 'true').lower() == 'true'
ENABLE_LANGFUSE = os.environ.get('ENABLE_LANGFUSE', 'true').lower() == 'true'

def setup_logging():
    """Set up simplified logging configuration."""
    logger = logging.getLogger('omcp')
    
    if not ENABLE_LOGGING:
        logger.addHandler(logging.NullHandler())
        return logger
    
    # Clear existing handlers
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    
    # Get log file path (simplified)
    log_file = os.environ.get('LOG_FILE', 'omcp.log')
    if not os.path.isabs(log_file):
        # Default to home directory logs folder
        log_dir = Path.home() / '.omcp' / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / log_file
    
    try:
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(file_handler)
        
        # Console handler for development
        if os.environ.get('DEBUG', 'false').lower() == 'true':
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(
                logging.Formatter('%(levelname)s - %(message)s')
            )
            logger.addHandler(console_handler)
            
        logger.info(f"Logging initialized - file: {log_file}")
        
    except Exception as e:
        # Fallback to console only
        console_handler = logging.StreamHandler()
        logger.addHandler(console_handler)
        logger.warning(f"Could not create log file, using console: {e}")
    
    return logger

# Initialize logger
logger = setup_logging()

# Initialize Langfuse (simplified)
langfuse = None
if ENABLE_LANGFUSE:
    try:
        langfuse = Langfuse(
            public_key=os.environ.get("LANGFUSE_PUBLIC_KEY"),
            secret_key=os.environ.get("LANGFUSE_SECRET_KEY"),
            host=os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com")
        )
        logger.info("Langfuse initialized")
    except Exception as e:
        logger.error(f"Langfuse initialization failed: {e}")

# Export observe decorator
observe = observe if langfuse else lambda *args, **kwargs: lambda func: func