import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)
else:

    load_dotenv()
    if not os.getenv("POSTGRES_DBNAME"): 
         print("Warning: .env file not found or not loaded correctly.")

# FOLDER PATHS
FOLDER_PATH = os.getenv("PDF_FOLDER_PATH")

# General Configuration
APP_NAME = "ielts_assistant"

# Celery Configuration
CELERY_BROKER_URL = os.getenv("CELERY_BROKER")
CELERY_RESULT_BACKEND = os.getenv("CELERY_BACKEND")

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = os.getenv("REDIS_DB")
REDIS_LOG_LIST_KEY_PREFIX = "chatlogs_list:"

# PostgreSQL Configuration
POSTGRES_DB_MIN_CONN = os.getenv("POSTGRES_DB_MIN_CONN")
POSTGRES_DB_MAX_CONN = os.getenv("POSTGRES_DB_MAX_CONN")
POSTGRES_DBNAME = os.getenv("POSTGRES_DBNAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST") 
POSTGRES_PORT = os.getenv("POSTGRES_PORT")     

# AWS S3 Configuration
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# Pinecone Configuration
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = "ielts-rag"
PINECONE_NAMESPACE = "ielts-passages"
PINECONE_INDEX_DIMENSION = 1024
PINECONE_INDEX_METRIC = "cosine"
PINECONE_INDEX_CLOUD = "aws"
PINECONE_INDEX_REGION = "us-east-1"
PINECONE_INDEX_MODEL = "multilingual-e5-large"

PINECONE_UPSERT_BATCH_SIZE = 100
TASK_FETCH_BATCH_SIZE = 1000
TASK_PROCESS_BATCH_SIZE = 100

#LLM Models Information
MISTRAL_MODEL_CHOICE = 'Mistral'
MISTRAL_MODEL = 'mistral-small-latest'
OPENAI_MODEL_CHOICE = 'GPT 4.1'
OPENAI_MODEL = 'gpt-4.1-2025-04-14'
DEEPSEEK_MODEL_CHOICE = 'DeepSeekR1'
DEEPSEEK_MODEL = 'deepseek/deepseek-r1:free'
DEEPSEEK_BASEURL = os.getenv("DEEPSEEK_BASEURL")

# LLM API Keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY") 

# Logging Configuration
LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
LOG_FILENAME = os.path.join(LOG_DIR, 'ielts_app.log')
LOG_LEVEL = logging.INFO  # Default level
LOG_FORMAT = '%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s] - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 3
LOG_BUFFER_THRESHOLD = 100
LOG_BUFFER_TTL_SECONDS = 3600           # Time-to-time live for Redis log buffer

# Logging Setup Function
def setup_logging():
    """Configures and returns the main application logger."""

    # Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(APP_NAME)
    logger.setLevel(LOG_LEVEL)

    # Prevent adding multiple handlers if called more than once
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # Rotating File Handler
    file_handler = RotatingFileHandler(
        LOG_FILENAME,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Optional: Console Handler (for development/debugging)
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.DEBUG) # Lower level for console if needed
    # console_handler.setFormatter(formatter)
    # logger.addHandler(console_handler)

    logger.info("Logging setup complete. Logging to %s", LOG_FILENAME)
    return logger