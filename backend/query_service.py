import json
import re
import config
from pinecone import Pinecone
from mistralai import Mistral
from openai import OpenAI

import logging
logger = logging.getLogger(config.APP_NAME)

# Celery App Import 
try:
    from celery_app import celery_app
    logger.info("celery_app has been successfully imported from Celery")

except ImportError as e:
    logger.exception("An exception has occurred when importing celery_app instance."
                     "Ensure celery.py exist in the project root. Error: %s", e)
    raise

# Status Constants
from enum import Enum

class ProcessingStatus(Enum):
    PINECONE_INIT_FAILED = 'pinecone_initialization_failed'
    LLM_INIT_FAILED = 'llm_initialization_failed'
    PINECONE_QUERY_FAILED = 'pinecone_query_failed'
    PASSAGE_GEN_FAILED = 'passage_generation_failed'
    QUESTION_GEN_FAILED = 'question_generation_failed'
    MAIN_EXEC_FAILED = 'main_execution_failed'
    UNKNOWN_ERROR = 'unknown_error' 

def validate_status(status):
    if isinstance(status, ProcessingStatus):
        return status.value
    
    raise ValueError(f"Invalid status: {status}. Must be in accordance to ProcessingStatus")

# Core Functions:
def initialize_pinecone():
    try:
        pinecone_api_key = config.PINECONE_API_KEY
        pinecone_index_name = config.PINECONE_INDEX_NAME

        if not pinecone_api_key:
            logging.error("An error has occurred. Pinecone API key not found")
            return None, None
        
        if not pinecone_index_name:
            logging.error("An error has occurred. Pinecone index not found in configuration")
            return None, None

        pc = Pinecone(api_key=pinecone_api_key)
        logging.debug(f"Verifying Pinecone Index. Create one if needed.")

        if pinecone_index_name not in [idx.name for idx in pc.list_indexes()]:
            logging.info(f"Pinecone Index {index_name} not found. Creating one")
            pc.create_index(
                name = pinecone_index_name,
                cloud = config.PINECONE_INDEX_CLOUD,
                region = config.PINECONE_INDEX_REGION,
                embed={
                    "model": "text-embedding-multilingual-e5-large",
                    "field_map": {"text": "text"}
                }
            )
            print(f"Index {index_name} created in Pinecone")
        
        else:
            print(f"Index {index_name} already exists in Pinecone")

        index = pc.Index(index_name)
        print(f"Connecting to Pinecone Index object 'index_name'.")

    except Exception as e:
        print(f"An error has occured during Pinecone initialization process: {e}")
        return None, None
    
    return pc, index